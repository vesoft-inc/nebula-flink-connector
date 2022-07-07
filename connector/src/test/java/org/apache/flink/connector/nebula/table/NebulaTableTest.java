package org.apache.flink.connector.nebula.table;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * make sure your environment has creates space, and data has been insert into this space.
 * Space schema:
 *
 * <p>"CREATE SPACE `testFlinkSource` (partition_num = 100, replica_factor = 3, charset = utf8,
 * collate = utf8_bin, vid_type = INT64, atomic_edge = false)"
 *
 * <p>"USE `testFlinkSource`"
 *
 * <p>"CREATE TAG IF NOT EXISTS person(col1 string, col2 fixed_string(8), col3 int8, col4 int16,
 * col5 int32, col6 int64, col7 date, col8 datetime, col9 timestamp, col10 bool, col11 double,
 * col12 float, col13 time, col14 geography);"
 *
 * <p>"CREATE EDGE IF NOT EXISTS friend(col1 string, col2 fixed_string(8), col3 int8, col4 int16,
 * col5 int32, col6 int64, col7 date, col8 datetime, col9 timestamp, col10 bool, col11 double,
 * col12 float, col13 time, col14 geography);"
 *
 * <p>"CREATE SPACE `testFlinkSink` (partition_num = 100, replica_factor = 3, charset = utf8,
 * collate = utf8_bin, vid_type = INT64, atomic_edge = false)"
 *
 * <p>"USE `testFlinkSink`"
 *
 * <p>"CREATE TAG IF NOT EXISTS person(col1 string, col2 fixed_string(8), col3 int8, col4 int16,
 * col5 int32, col6 int64, col7 date, col8 datetime, col9 timestamp, col10 bool, col11 double,
 * col12 float, col13 time, col14 geography);"
 *
 * <p>"CREATE EDGE IF NOT EXISTS friend(col1 string, col2 fixed_string(8), col3 int8, col4 int16,
 * col5 int32, col6 int64, col7 date, col8 datetime, col9 timestamp, col10 bool, col11 double,
 * col12 float, col13 time, col14 geography);"
 */
public class NebulaTableTest {
    private static final Logger log = LoggerFactory.getLogger(NebulaTableTest.class);

    @Test
    public void testVertexTransfer() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        String creatSourceDDL = "CREATE TABLE `VERTEX.personSource` ("
                + " vid BIGINT,"
                + " col1 STRING,"
                + " col2 STRING,"
                + " col3 INT,"
                + " col4 INT,"
                + " col5 INT,"
                + " col6 BIGINT,"
                + " col7 DATE,"
                + " col8 TIMESTAMP,"
                + " col9 BIGINT,"
                + " col10 BOOLEAN,"
                + " col11 DOUBLE,"
                + " col12 DOUBLE,"
                + " col13 TIME,"
                + " col14 STRING"
                + ") WITH ("
                + " 'connector' = 'nebula',"
                + " 'meta-address' = '192.168.200.135:9559',"
                + " 'graph-address' = '192.168.200.135:9669',"
                + " 'username' = 'root',"
                + " 'password' = 'nebula',"
                + " 'graph-space' = 'testFlinkSource',"
                + " 'label-name' = 'person'"
                + ")";
        tableEnv.executeSql(creatSourceDDL);

        String creatSinkDDL = "CREATE TABLE `VERTEX.personSink` ("
                + " vid BIGINT,"
                + " col1 STRING,"
                + " col2 STRING,"
                + " col3 INT,"
                + " col4 INT,"
                + " col5 INT,"
                + " col6 BIGINT,"
                + " col7 DATE,"
                + " col8 TIMESTAMP,"
                + " col9 BIGINT,"
                + " col10 BOOLEAN,"
                + " col11 DOUBLE,"
                + " col12 DOUBLE,"
                + " col13 TIME,"
                + " col14 STRING"
                + ") WITH ("
                + " 'connector' = 'nebula',"
                + " 'meta-address' = '192.168.200.135:9559',"
                + " 'graph-address' = '192.168.200.135:9669',"
                + " 'username' = 'root',"
                + " 'password' = 'nebula',"
                + " 'graph-space' = 'testFlinkSink',"
                + " 'label-name' = 'person'"
                + ")";
        tableEnv.executeSql(creatSinkDDL);

        Table table = tableEnv.sqlQuery("SELECT * FROM `VERTEX.personSource`");
        table.executeInsert("`VERTEX.personSink`");
    }

    @Test
    public void testEdgeTransfer() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        String creatSourceDDL = "CREATE TABLE `EDGE.friendSource` ("
                + " sid BIGINT,"
                + " eid BIGINT,"
                + " rid BIGINT,"
                + " col1 STRING,"
                + " col2 STRING,"
                + " col3 INT,"
                + " col4 INT,"
                + " col5 INT,"
                + " col6 BIGINT,"
                + " col7 DATE,"
                + " col8 TIMESTAMP,"
                + " col9 BIGINT,"
                + " col10 BOOLEAN,"
                + " col11 DOUBLE,"
                + " col12 DOUBLE,"
                + " col13 TIME,"
                + " col14 STRING"
                + ") WITH ("
                + " 'connector' = 'nebula',"
                + " 'meta-address' = '192.168.200.135:9559',"
                + " 'graph-address' = '192.168.200.135:9669',"
                + " 'username' = 'root',"
                + " 'password' = 'nebula',"
                + " 'graph-space' = 'testFlinkSource',"
                + " 'label-name' = 'friend',"
                + " 'src-index' = '0',"
                + " 'dst-index' = '1',"
                + " 'rank-index' = '2'"
                + ")";
        tableEnv.executeSql(creatSourceDDL);

        String creatSinkDDL = "CREATE TABLE `EDGE.friendSink` ("
                + " sid BIGINT,"
                + " eid BIGINT,"
                + " rid BIGINT,"
                + " col1 STRING,"
                + " col2 STRING,"
                + " col3 INT,"
                + " col4 INT,"
                + " col5 INT,"
                + " col6 BIGINT,"
                + " col7 DATE,"
                + " col8 TIMESTAMP,"
                + " col9 BIGINT,"
                + " col10 BOOLEAN,"
                + " col11 DOUBLE,"
                + " col12 DOUBLE,"
                + " col13 TIME,"
                + " col14 STRING"
                + ") WITH ("
                + " 'connector' = 'nebula',"
                + " 'meta-address' = '192.168.200.135:9559',"
                + " 'graph-address' = '192.168.200.135:9669',"
                + " 'username' = 'root',"
                + " 'password' = 'nebula',"
                + " 'graph-space' = 'testFlinkSink',"
                + " 'label-name' = 'friend',"
                + " 'src-index' = '0',"
                + " 'dst-index' = '1',"
                + " 'rank-index' = '2'"
                + ")";
        tableEnv.executeSql(creatSinkDDL);

        Table table = tableEnv.sqlQuery("SELECT * FROM `EDGE.friendSource`");
        table.executeInsert("`EDGE.friendSink`");
    }
}
