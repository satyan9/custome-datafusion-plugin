package com.example.plugins;

import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.etl.api.*;
import io.cdap.cdap.etl.api.batch.BatchSource;
import org.apache.hadoop.io.NullWritable;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name("HttpPaginationBatchSource")
@Description("Fetches multiple API endpoints using a parameter list with pagination support.")
public class HttpPaginationBatchSource extends BatchSource<NullWritable, String, StructuredRecord> {

    public static class Config extends PluginConfig {
        @Name("apiTemplate")
        @Description("API URL template with ${param} placeholder, e.g., https://api.com/data?id=${param}")
        public String apiTemplate;

        @Name("paramsFilePath")
        @Description("GCS path to a file containing a list of parameters (e.g., gs://bucket/file.json or gs://bucket/file.csv)")
        public String paramsFilePath;

        @Name("paginationParam")
        @Description("Pagination parameter name (e.g., page), optional")
        public String paginationParam;

        @Name("maxPages")
        @Description("Maximum pages to fetch if pagination is enabled")
        public Integer maxPages;

        public void validate() {
            // Add validation logic here, e.g., checking for required fields
        }
    }

    private final Config config;

    public HttpPaginationBatchSource(Config config) {
        this.config = config;
    }

    @Override
    public void prepareRun(BatchSourceContext context) throws Exception {
        config.validate();

        // 1. Read the parameter file from GCS on the driver node.
        String[] pathParts = config.paramsFilePath.replace("gs://", "").split("/", 2);
        String bucketName = pathParts[0];
        String blobName = pathParts[1];

        Storage storage = StorageOptions.getDefaultInstance().getService();
        String fileContent;
        try (InputStreamReader isr = new InputStreamReader(Channels.newInputStream(storage.reader(BlobId.of(bucketName, blobName))), StandardCharsets.UTF_8);
             BufferedReader reader = new BufferedReader(isr)) {
            fileContent = reader.lines().collect(Collectors.joining(System.lineSeparator()));
        }

        JsonArray paramsArray = JsonParser.parseString(fileContent).getAsJsonArray();
        
        // 2. Create an Input for each parameter to distribute the work across worker nodes.
        for (int i = 0; i < paramsArray.size(); i++) {
            String param = paramsArray.get(i).getAsString();
            context.setInput(Input.of(param));
        }

        // We can't set an output schema at design time without knowing the API response structure.
        // It's better to let the next pipeline stage (e.g., a Wrangler) define the schema.
        context.setOutputSchema(null);
    }

    @Override
    public void transform(KeyValue<NullWritable, String> input,
                          Emitter<StructuredRecord> emitter) throws Exception {
        // 3. The 'transform' method receives a single parameter to process.
        // This runs on a worker node.
        String param = input.getValue();

        String urlStr = config.apiTemplate.replace("${param}", param);

        if (config.paginationParam != null && config.maxPages != null) {
            for (int i = 1; i <= config.maxPages; i++) {
                String pageUrl = urlStr + "&" + config.paginationParam + "=" + i;
                fetchAndEmit(pageUrl, emitter);
            }
        } else {
            fetchAndEmit(urlStr, emitter);
        }
    }

    private void fetchAndEmit(String urlStr, Emitter<StructuredRecord> emitter) throws Exception {
        URL url = new URL(urlStr);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setRequestProperty("User-Agent", "Mozilla/5.0"); // Some APIs require this

        try (BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
            StringBuilder content = new StringBuilder();
            String line;
            while ((line = in.readLine()) != null) {
                content.append(line);
            }
            
            // Simplified: emit the whole JSON response as a single structured record
            Schema schema = Schema.recordOf(
                "api_response",
                Schema.Field.of("response", Schema.of(Schema.Type.STRING))
            );
            StructuredRecord record = StructuredRecord.builder(schema)
                .set("response", content.toString())
                .build();
            emitter.emit(record);

        } finally {
            conn.disconnect();
        }
    }
}