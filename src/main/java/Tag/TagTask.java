package Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import com.aliyun.odps.utils.StringUtils;
import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.net.NebulaPool;
import com.vesoft.nebula.client.graph.net.Session;


public class TagTask implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(TagTask.class);
    private int offset;
    private Properties props;
    private List<String> tagNames;
    private NebulaPool pool;

    private Session session = null;
    private Statement stmt = null;

    public TagTask(int offset, Properties props, List<String> tagNames, NebulaPool pool) {
        this.offset = offset;
        this.props = props;
        this.tagNames = tagNames;
        this.pool = pool;
    }

    private void insertTag() {
        String tableName = props.getProperty("tableName");
        String partition = props.getProperty("partition");
        int batchSize = Integer.parseInt(props.getProperty("batchSize", "1000"));

        String select1 = "SELECT concat(id1_type, id1) as vid, id1 as id FROM " + tableName + " WHERE fq_day = '" + partition + "' AND id1_type = '";
        String select2 = "SELECT concat(id2_type, id2) as vid, id2 as id FROM " + tableName + " WHERE fq_day = '" + partition + "' AND id2_type = '";

        java.sql.ResultSet result;
        try {
            for (String tagName : tagNames) {
                StringBuilder sql = new StringBuilder(256);
                sql.append(select1 + tagName + "'  LIMIT " + offset + " , " + batchSize + " UNION " + select2 + tagName + "' LIMIT " + offset + " , " + batchSize);
                result = stmt.executeQuery(sql.toString());
                if (!result.next()) {
                    continue;
                }
                StringBuilder ngql = new StringBuilder(2 * batchSize);
                ngql.append("INSERT VERTEX " + convertToTagName(tagName) + "(id) VALUES ");
                do {
                    String vid = result.getString("vid");
                    String id = result.getString("id");
                    ngql.append("hash('" + vid + "'):('" + id + "'),");
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

            insertTag();
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

            long endTime = System.currentTimeMillis();
            Thread t = Thread.currentThread();
            logger.info("TAG ThreadID: {}, OFFSET: {} Time : {}", t.getId(), offset, ((endTime - startTime) + "ms"));
        }
    }

    public static String convertToTagName(String name) {
        String [] temp = name.split("-");
        String temp1 = StringUtils.join(temp, "_");
        StringBuilder res = new StringBuilder(temp1);
        res.insert(0, "v");
        return res.toString();
    }
}