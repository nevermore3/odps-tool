import java.io.File;
import java.sql.*;
import java.util.Arrays;
import java.util.List;

import com.google.gson.*;
import com.vesoft.nebula.client.graph.NebulaPoolConfig;
import com.vesoft.nebula.client.graph.data.HostAddress;
import com.vesoft.nebula.client.graph.net.NebulaPool;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


import work.WorkTask;

public class odps {
    private static final Logger logger = LoggerFactory.getLogger(odps.class);
    private static String driverName = "com.aliyun.odps.jdbc.OdpsDriver";

    public static void main(String[] args) throws SQLException {
        long startTime = System.currentTimeMillis();
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        if (args.length != 1) {
            logger.error("NO CONFIG FILE PATH");
            System.exit(1);
        }
        NebulaPool pool = null;
        ExecutorService threadPool;
        JsonObject jsonObject;
        try {
            String filePath = args[0];
            String configFileName = filePath + "/db.json";
            jsonObject = readJsonFile(configFileName);

            int threadPoolSize = jsonObject.get("threadPoolSize").getAsInt();
            logger.info("Init ThreadPool Size :" + threadPoolSize);
            threadPool = Executors.newFixedThreadPool(threadPoolSize);

            logger.info("Build ODPS Connect");
            JsonObject odpsObject = jsonObject.getAsJsonObject("odps");
            String accessId = odpsObject.get("accessId").getAsString();
            String accessKey = odpsObject.get("accessKey").getAsString();
            String jdbcUrl = odpsObject.get("jdbcUrl").getAsString();
            Connection conn = DriverManager.getConnection(jdbcUrl, accessId, accessKey);
            Statement stmt = conn.createStatement();

            JsonObject nebulaObject = jsonObject.getAsJsonObject("nebula");
            int port = nebulaObject.get("port").getAsInt();
            String address = nebulaObject.get("graphAddress").getAsString();
            int connectSize = nebulaObject.get("connectSize").getAsInt();
            NebulaPoolConfig nebulaPoolConfig = new NebulaPoolConfig();
            nebulaPoolConfig.setMaxConnSize(connectSize);
            List<HostAddress> addresses = Arrays.asList(new HostAddress(address, port));
            pool = new NebulaPool();
            boolean initSuccess = pool.init(addresses, nebulaPoolConfig);
            if (!initSuccess) {
                logger.error("Nebula Pool Init Error");
                System.exit(1);
            }

            // get table's size
            JsonArray dataArray = jsonObject.get("data").getAsJsonArray();
            for (int i = 0; i < dataArray.size(); ++i) {
                JsonObject dataObject = dataArray.get(i).getAsJsonObject();
                String linesSql = dataObject.get("totalLinesSql").getAsString();
                String tableName = dataObject.get("odpsTableName").getAsString();
                java.sql.ResultSet rs;
                rs = stmt.executeQuery(linesSql);
                if (!rs.next()) {
                    logger.error("EMPTY TABLE : " + tableName);
                    dataObject.addProperty("totalLines", 0);
                    continue;
                }
                long lines = rs.getInt(1);
                logger.info("Table {} , total lines : {}", tableName, lines);
                dataObject.addProperty("totalLines", lines);
            }

            int batchSize = odpsObject.get("batchSize").getAsInt();

            for (int i = 0; i < dataArray.size(); ++i) {
                JsonObject dataObject = dataArray.get(i).getAsJsonObject();
                int totalLines = dataObject.get("totalLines").getAsInt();
                for (long offset = 0; offset < totalLines; offset += batchSize) {
                    threadPool.execute(new WorkTask(offset, dataObject, jsonObject, pool, batchSize));
                }
            }
            threadPool.shutdown();
            try {
                threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                logger.error(e.getMessage());
            }

        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        } finally {
            pool.close();
            long endTime = System.currentTimeMillis();
            logger.info("OVER ,Total Time: " + ((endTime - startTime) / 1000 + "s"));
        }
    }


    public static JsonObject readJsonFile(String filePath) {
        JsonObject jsonObject = null;
        try {
            File file=new File(filePath);
            String jsonString= FileUtils.readFileToString(file,"UTF-8");
            JsonParser parser = new JsonParser();
            JsonElement jsonNode = parser.parse(jsonString);
            if (jsonNode.isJsonObject()) {
                jsonObject = jsonNode.getAsJsonObject();
            } else {
                logger.error("Parser Json {} Error", filePath);
                System.exit(1);
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }
        return jsonObject;
    }
}
