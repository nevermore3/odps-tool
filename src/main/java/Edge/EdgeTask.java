package Edge;
import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.net.NebulaPool;
import com.vesoft.nebula.client.graph.net.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;


public class EdgeTask implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(Tag.TagTask.class);
    private int offset;
    private Properties props;
    private List<String> edgeNames;
    private NebulaPool pool;

    private Session session = null;
    private Statement stmt = null;

    public EdgeTask(int offset, Properties props, List<String> edgeNames, NebulaPool pool) {
        this.offset = offset;
        this.props = props;
        this.edgeNames = edgeNames;
        this.pool = pool;
    }

    private void insertEdge() {
        String tableName = props.getProperty("tableName");
        String partition = props.getProperty("partition");
        int batchSize = Integer.parseInt(props.getProperty("batchSize", "1000"));

        String select1 = "SELECT concat(id1_type, id1) as src, concat(id2_type, id2) as dst, gxlxdm, gxlxmc, gxkssj, gxjssj, gxcs FROM " + tableName;
        String where1 = " WHERE fq_day = '" + partition + "' AND gxlxdm LIKE ";

        java.sql.ResultSet result;
        try {
            for (String edgeName : edgeNames) {
                StringBuilder sql = new StringBuilder(128);
                sql.append(select1 + where1 + " '" + edgeName + "%' LIMIT " + offset + "," + batchSize);
                result = stmt.executeQuery(sql.toString());
                if (!result.next()) {
                    continue;
                }
                StringBuilder ngql = new StringBuilder(2 * batchSize);
                ngql.append("INSERT EDGE " + convertToEdgeName(edgeName) + "(type, name, begin, end, count) VALUES ");
                do {
                    String src = result.getString("src");
                    String dst = result.getString("dst");
                    String type = result.getString("gxlxdm");
                    String name = result.getString("gxlxmc");
                    String begin = result.getString("gxkssj");
                    String end = result.getString("gxjssj");
                    int count = result.getInt("gxcs");

                    ngql.append("hash('" + src + "')->hash('" + dst + "'):(" + type + "," + name + "," + begin + "," + end + "," + count + "),");
                } while (result.next());
                ngql.deleteCharAt(ngql.length() - 1);
                ResultSet resp = session.execute(ngql.toString());
                if (!resp.isSucceeded()) {
                    logger.error(resp.getErrorMessage());
                }
                logger.info("Running " + ngql);
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }
    }

    @Override
    public void run() {
        long startTime = System.currentTimeMillis();
        // odps connection
        Connection conn = null;
        try {
            String accessId = props.getProperty("accessId");
            String accessKey = props.getProperty("accessKey");
            String jdbcUrl = props.getProperty("jdbcUrl");
            conn = DriverManager.getConnection(jdbcUrl, accessId, accessKey);
            stmt = conn.createStatement();

            String account = props.getProperty("account");
            String password = props.getProperty("password");
            String spaceName = props.getProperty("spaceName");
            session = pool.getSession(account, password, false);
            ResultSet resp = session.execute("use " + spaceName);

            insertEdge();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        } finally {
            try {
                stmt.close();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
            try {
                conn.close();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
            if (session != null) {
                session.release();
            }
            pool.close();

            long endTime = System.currentTimeMillis();
            Thread t = Thread.currentThread();
            logger.info("EDGE ThreadID: {}, OFFSET: {} Time: {}", t.getId(), offset, ((endTime - startTime) + "ms"));
        }
    }

    public static String convertToEdgeName(String name) {
        StringBuilder res = new StringBuilder(name);
        res.insert(0, "e");
        return res.toString();
    }
}
