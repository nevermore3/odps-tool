import java.sql.*;
import java.util.Arrays;
import java.util.List;

import com.vesoft.nebula.client.graph.NebulaPoolConfig;
import com.vesoft.nebula.client.graph.data.HostAddress;
import com.vesoft.nebula.client.graph.net.NebulaPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
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
        try {
            String filePath = args[0];
            String configFileName = filePath + "/db.properties";

            Properties props = new Properties();
            props.load(new java.io.FileInputStream(configFileName));
            int threadPoolSize = Integer.parseInt(props.getProperty("threadPoolSize"));
            logger.info("Init ThreadPool Size :" + threadPoolSize);
            threadPool = Executors.newFixedThreadPool(threadPoolSize);

            logger.info("Build ODPS Connect");
            String accessId = props.getProperty("accessId");
            String accessKey = props.getProperty("accessKey");
            String jdbcUrl = props.getProperty("jdbcUrl");
            Connection conn = DriverManager.getConnection(jdbcUrl, accessId, accessKey);
            Statement stmt = conn.createStatement();

            String tableName = props.getProperty("tableName");
            String partition = props.getProperty("partition");
            String lineSql = "SELECT COUNT(1) FROM " + tableName + " WHERE fq_day = " + partition;
            java.sql.ResultSet rs;
            rs = stmt.executeQuery(lineSql);
            if (!rs.next()) {
                logger.error("EMPTY TABLE");
                System.exit(1);
            }
            long lines = rs.getInt(1);
            logger.info("TOTAL LINES : " + lines);

            int batchSize = Integer.parseInt(props.getProperty("batchSize", "1000"));

            int port = Integer.parseInt(props.getProperty("port"));
            String address = props.getProperty("graphAddress");
            int connectSize = Integer.parseInt(props.getProperty("connectSize"));
            NebulaPoolConfig nebulaPoolConfig = new NebulaPoolConfig();
            nebulaPoolConfig.setMaxConnSize(connectSize);
            List<HostAddress> addresses = Arrays.asList(new HostAddress(address, port));
            pool = new NebulaPool();
            boolean initSuccess = pool.init(addresses, nebulaPoolConfig);
            if (!initSuccess) {
                logger.error("Nebula Pool Init Error");
                System.exit(1);
            }

            for (long offset = 0; offset < lines; offset += batchSize) {
                threadPool.execute(new WorkTask(offset, props, pool));
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


    public static List<String> readFile(String filePath) {
        List<String> list = new java.util.ArrayList<String>();
        try (BufferedReader br = Files.newBufferedReader(Paths.get(filePath))) {
            //br returns as stream and convert it into a List
            list = br.lines().collect(Collectors.toList());
        } catch (IOException e) {
            e.printStackTrace();
        }
        List<String> filtered = list.stream().filter(string -> !string.isEmpty()).collect(Collectors.toList());
        filtered.forEach(System.out::println);
        return filtered;
    }
}
