package work;
import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.net.NebulaPool;
import com.vesoft.nebula.client.graph.net.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class WorkTask implements Runnable {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private long offset;
    private Properties props;
    private NebulaPool pool;

    private Session session = null;
    private Statement stmt = null;

    public WorkTask(long offset, Properties props, NebulaPool pool) {
        this.offset = offset;
        this.props = props;
        this.pool = pool;
    }

    private void insertTag(Map<String, Set<String>> tagMap) {
        long startTime = System.currentTimeMillis();
        try {
            for (Map.Entry<String, Set<String>> entry : tagMap.entrySet()) {
                String tagType = entry.getKey();
                Set<String> idSet = entry.getValue();
                int size = idSet.size();
                StringBuilder ngql = new StringBuilder(size * 40);
                ngql.append("INSERT VERTEX " + convertToTagName(tagType) + "(id) VALUES ");
                for (String id : idSet) {
                    ngql.append("hash('" + tagType + id + "'):('" + id + "'),");
                }
                ngql.deleteCharAt(ngql.length() - 1);
                ResultSet resp = session.execute(ngql.toString());
                if (!resp.isSucceeded()) {
                    logger.error(resp.getErrorMessage());
                    logger.error("Run " + ngql);
                }
                logger.info("Insert Vertex: {}, Size: {}", tagType, size);
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        } finally {
            long endTime = System.currentTimeMillis();
            Thread t = Thread.currentThread();
            logger.info("TAG ThreadID: {}, OFFSET: {}, Time : {}", t.getId(), offset, ((endTime - startTime) + "ms"));
        }
    }

    private void insertEdge(Map<String, Set<Map<String, String>>> edgeMap) {
        long startTime = System.currentTimeMillis();
        try {
            for (Map.Entry<String, Set<Map<String, String>>> entry : edgeMap.entrySet()) {
                String edgeType = entry.getKey();
                Set<Map<String, String>> edgeInfos = entry.getValue();
                int size = edgeInfos.size();
                StringBuilder ngql = new StringBuilder(size * 100);
                ngql.append("INSERT EDGE " + edgeType + "(type, name, begin, end, count) VALUES ");

                for (Map<String, String> edgeInfo : edgeInfos) {
                    ngql.append("hash('" + edgeInfo.get("src") + "')->hash('" + edgeInfo.get("dst") + "'):");
                    ngql.append("('" + edgeInfo.get("type") + "','" + edgeInfo.get("name") + "','" + edgeInfo.get("begin") + "','" + edgeInfo.get("end") + "','" + edgeInfo.get("count") + "'),");
                }
                ngql.deleteCharAt(ngql.length() - 1);
                ResultSet resp = session.execute(ngql.toString());
                if (!resp.isSucceeded()) {
                    logger.error(resp.getErrorMessage());
                    logger.error("Run " + ngql);
                }
                logger.info("Insert Edge: {}, Size: {}", edgeType, size);
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        } finally {
            long endTime = System.currentTimeMillis();
            Thread t = Thread.currentThread();
            logger.info("EDGE ThreadID: {}, OFFSET: {}, Time: {}", t.getId(), offset, ((endTime - startTime) + "ms"));
        }

    }

    private void doWork() {
        String tableName = props.getProperty("tableName");
        String partition = props.getProperty("partition");
        int edgeCategory = Integer.parseInt(props.getProperty("edgeCategory"));
        int batchSize = Integer.parseInt(props.getProperty("batchSize", "1000000"));
        String sql = "SELECT id1_type, id1, id2_type, id2, gxlxdm, gxlxmc, gxkssj, gxjssj, gxcs FROM " + tableName + " WHERE fq_day = '" + partition + "' LIMIT " + offset + "," + batchSize;

        int insertSize = Integer.parseInt(props.getProperty("insertSize", "2000"));
        java.sql.ResultSet rs;
        try {
            rs = stmt.executeQuery(sql);
            do {
                Map<String, Set<String>> tagMap = new HashMap<>(1000);
                Map<String, Set<Map<String, String>>> edgeMap = new HashMap<>(100);
                int insertNum = 0;
                if (!rs.next()) {
                    logger.info("IT's END!!! offset : " + offset);
                    return;
                }
                // extract tag & edge & insert to tagMap or edgeMap
                do {
                    // process tag
                    String id1_type = rs.getString("id1_type");
                    String id1 = rs.getString("id1");
                    String id2_type = rs.getString("id2_type");
                    String id2 = rs.getString("id2");
                    if (tagMap.containsKey(id1_type)) {
                        Set<String> id1Set = tagMap.get(id1_type);
                        id1Set.add(id1);
                    } else {
                        Set<String> id1Set = new HashSet<>();
                        id1Set.add(id1);
                        tagMap.put(id1_type, id1Set);
                    }

                    if (tagMap.containsKey(id2_type)) {
                        Set<String> id2Set = tagMap.get(id2_type);
                        id2Set.add(id2);
                    } else {
                        Set<String> id2Set = new HashSet<>();
                        id2Set.add(id2);
                        tagMap.put(id2_type, id2Set);
                    }

                    // process edge
                    String edgeType;
                    String type = rs.getString("gxlxdm");
                    String name = rs.getString("gxlxmc");
                    String begin = rs.getString("gxkssj");
                    String end = rs.getString("gxjssj");
                    String count = rs.getString("gxcs");

                    if (edgeCategory == 0) {
                        edgeType = "e" + type.substring(0, type.indexOf("-"));
                    } else {
                        String temp = type.replace("-", "_");
                        edgeType = "e" + temp;
                    }
                    Map<String, String> edgeInfo = new HashMap<>(10);
                    edgeInfo.put("src", id1_type + id1);
                    edgeInfo.put("dst", id2_type + id2);
                    edgeInfo.put("type", type);
                    edgeInfo.put("name", name);
                    edgeInfo.put("begin", begin);
                    edgeInfo.put("end", end);
                    edgeInfo.put("count", count);

                    if (edgeMap.containsKey(edgeType)) {
                        Set<Map<String, String>> edgeInfos = edgeMap.get(edgeType);
                        edgeInfos.add(edgeInfo);
                    } else {
                        Set<Map<String, String>> edgeInfos = new HashSet<>();
                        edgeInfos.add(edgeInfo);
                        edgeMap.put(edgeType, edgeInfos);
                    }
                    insertNum += 1;
                    if (insertNum == insertSize) {
                        break;
                    }
                } while (rs.next());

                insertTag(tagMap);
                insertEdge(edgeMap);
            } while (rs.next());
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }
    }

    @Override
    public void run() {
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

            doWork();
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
        }
    }

    public static String convertToTagName(String name) {
        String temp = name.replace("-", "_");
        return "v" + temp;
    }
}