package work;
import com.google.gson.JsonObject;
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
    private JsonObject jsonObject;
    private JsonObject dataObject;
    private NebulaPool pool;
    private long batchSize;

    private Session session = null;
    private Statement stmt = null;

    public WorkTask(long offset, JsonObject dataObject, JsonObject jsonObject, NebulaPool pool, long batchSize) {
        this.offset = offset;
        this.jsonObject = jsonObject;
        this.dataObject = dataObject;
        this.pool = pool;
        this.batchSize = batchSize;
    }


    private void insertTag(Map<String, Set<String>> tagMap) {
        long startTime = System.currentTimeMillis();
        try {
            for (Map.Entry<String, Set<String>> entry : tagMap.entrySet()) {
                String tagType = entry.getKey();
                Set<String> idSet = entry.getValue();
                int size = idSet.size();
                StringBuilder ngql = new StringBuilder(size * 40);
                ngql.append("INSERT VERTEX " + tagType + "(id) VALUES ");
                for (String id : idSet) {
                    ngql.append("hash('" + id + "'):('" + id + "'),");
                }
                ngql.deleteCharAt(ngql.length() - 1);
                ResultSet resp = session.execute(ngql.toString());
                if (!resp.isSucceeded()) {
                    logger.error(resp.getErrorMessage());
                    logger.error("Run " + ngql);
                }
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
                ngql.append("INSERT EDGE " + edgeType + "(type, name, begin, end) VALUES ");

                for (Map<String, String> edgeInfo : edgeInfos) {
                    ngql.append("hash('" + edgeInfo.get("src") + "')->hash('" + edgeInfo.get("dst") + "')@" + edgeInfo.get("rank") + ":");
                    ngql.append("('" + edgeInfo.get("type") + "','" + edgeInfo.get("name") + "','" + edgeInfo.get("begin") + "','" + edgeInfo.get("end") + "'),");
                }
                ngql.deleteCharAt(ngql.length() - 1);
                ResultSet resp = session.execute(ngql.toString());
                if (!resp.isSucceeded()) {
                    logger.error(resp.getErrorMessage());
                    logger.error("Run " + ngql);
                }
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
        int insertSize = jsonObject.getAsJsonObject("nebula").get("insertSize").getAsInt();
        String sql = dataObject.get("odpsDataSql").getAsString() + " LIMIT " + offset + "," + batchSize;
        boolean split = dataObject.get("split").getAsBoolean();

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
                    String begin = rs.getString("gxkssj");
                    if (id1 != null && !id1.contains("'")) {
                        if (tagMap.containsKey(id1_type)) {
                            Set<String> id1Set = tagMap.get(id1_type);
                            id1Set.add(id1);
                        } else {
                            Set<String> id1Set = new HashSet<>();
                            id1Set.add(id1);
                            tagMap.put(id1_type, id1Set);
                        }
                    }

                    if (id2 != null && !id2.contains("'")) {
                        if (tagMap.containsKey(id2_type)) {
                            Set<String> id2Set = tagMap.get(id2_type);
                            id2Set.add(id2);
                        } else {
                            Set<String> id2Set = new HashSet<>();
                            id2Set.add(id2);
                            tagMap.put(id2_type, id2Set);
                        }
                    }
                    if (id1 == null || id2 == null || id1.contains("'") || id2.contains("'")) {
                        if (id1.contains("'")) {
                            logger.error("id1 error " + id1);
                        }
                        if (id2.contains("'")) {
                            logger.error("id2 error " + id2);
                        }
                        continue;
                    }

                    // process edge
                    String type = rs.getString("gxlxdm");
                    String name = rs.getString("gxlxmc");
                    String end = rs.getString("gxjssj");
                    String rank = rs.getString("rank");
                    if (rank == null) {
                        logger.error("rank error, begin is : " + begin);
                        continue;
                    }

                    Map<String, String> edgeInfo = new HashMap<>(10);
                    edgeInfo.put("src",  id1);
                    edgeInfo.put("dst",  id2);
                    edgeInfo.put("type", type);
                    edgeInfo.put("name", name);
                    edgeInfo.put("begin", begin);
                    edgeInfo.put("end", end);
                    edgeInfo.put("rank", rank);

                    if (edgeMap.containsKey(type)) {
                        Set<Map<String, String>> edgeInfos = edgeMap.get(type);
                        edgeInfos.add(edgeInfo);
                    } else {
                        Set<Map<String, String>> edgeInfos = new HashSet<>();
                        edgeInfos.add(edgeInfo);
                        edgeMap.put(type, edgeInfos);
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
            JsonObject odpsObject = jsonObject.getAsJsonObject("odps");
            String accessId = odpsObject.get("accessId").getAsString();
            String accessKey = odpsObject.get("accessKey").getAsString();
            String jdbcUrl = odpsObject.get("jdbcUrl").getAsString();
            conn = DriverManager.getConnection(jdbcUrl, accessId, accessKey);
            stmt = conn.createStatement();

            JsonObject nebulaObject = jsonObject.getAsJsonObject("nebula");
            String account = nebulaObject.get("account").getAsString();
            String password = nebulaObject.get("password").getAsString();
            String spaceName = nebulaObject.get("spaceName").getAsString();
            session = pool.getSession(account, password, false);
            ResultSet resp = session.execute("use " + spaceName);
            if (!resp.isSucceeded()) {
                logger.error("NEBULA SPACE {} NOT EXISTS", spaceName);
                System.exit(1);
            }
            doWork();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        } finally {
            try {
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
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