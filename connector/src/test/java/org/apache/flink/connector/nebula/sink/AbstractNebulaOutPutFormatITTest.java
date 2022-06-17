package org.apache.flink.connector.nebula.sink;

import java.util.concurrent.ExecutionException;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.Test;

public class AbstractNebulaOutPutFormatITTest {
    static final String META_ADDRESS = "127.0.0.1:9559";
    static final String GRAPH_ADDRESS = "127.0.0.1:9669";
    static final String USER_NAME = "root";
    static final String PASSWORD = "nebula";

    /** sink Nebula Graph Vertex Data with default INSERT mode */
    @Test
    public void sinkVertexData() throws ExecutionException, InterruptedException {
        TableEnvironment tableEnvironment =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        tableEnvironment.executeSql(
                "CREATE TABLE person ("
                        + "vid STRING,"
                        + "col1 STRING,"
                        + "col2 STRING,"
                        + "col3 STRING,"
                        + "col4 STRING,"
                        + "col5 STRING,"
                        + "col6 STRING,"
                        + "col7 STRING,"
                        + "col8 STRING,"
                        + "col9 STRING,"
                        + "col10 STRING,"
                        + "col11 STRING,"
                        + "col12 STRING,"
                        + "col13 STRING,"
                        + "col14 STRING"
                        + ")"
                        + "WITH ("
                        + "'connector'='nebula',"
                        + "'meta-address'='"
                        + META_ADDRESS
                        + "',"
                        + "'graph-address'='"
                        + GRAPH_ADDRESS
                        + "',"
                        + "'username'='"
                        + USER_NAME
                        + "',"
                        + "'password'='"
                        + PASSWORD
                        + "',"
                        + "'graph-space'='flinkSink',"
                        + "'write-mode'='insert',"
                        + "'data-type'='vertex'"
                        + ")");

        tableEnvironment
                .executeSql(
                        "insert into person values ('89', 'aba', 'abcdefgh', '1', '1111',"
                                + " '22222', '6412233', '2019-01-01', '2019-01-01T12:12:12',"
                                + " '435463424', 'false', '1.2', '1.0', '11:12:12', 'POINT(1 3)')")
                .await();
    }

    /** sink Nebula Graph Edge Data with default INSERT mode */
    @Test
    public void sinkEdgeData() throws ExecutionException, InterruptedException {
        TableEnvironment tableEnvironment =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        tableEnvironment.executeSql(
                "CREATE TABLE friend ("
                        + "src STRING,"
                        + "dst STRING,"
                        + "col1 STRING,"
                        + "col2 STRING,"
                        + "col3 STRING,"
                        + "col4 STRING,"
                        + "col5 STRING,"
                        + "col6 STRING,"
                        + "col7 STRING,"
                        + "col8 STRING,"
                        + "col9 STRING,"
                        + "col10 STRING,"
                        + "col11 STRING,"
                        + "col12 STRING,"
                        + "col13 STRING,"
                        + "col14 STRING"
                        + ")"
                        + "WITH ("
                        + "'connector'='nebula',"
                        + "'meta-address'='"
                        + META_ADDRESS
                        + "',"
                        + "'graph-address'='"
                        + GRAPH_ADDRESS
                        + "',"
                        + "'username'='"
                        + USER_NAME
                        + "',"
                        + "'password'='"
                        + PASSWORD
                        + "',"
                        + "'graph-space'='flinkSink',"
                        + "'src-index'='0',"
                        + "'dst-index'='1',"
                        + "'rank-index'='4',"
                        + "'write-mode'='insert',"
                        + "'data-type'='edge'"
                        + ")");

        tableEnvironment
                .executeSql(
                        "insert into friend values ('61', '62', 'aba', 'abcdefgh',"
                                + " '1', '1111', '22222', '6412233', '2019-01-01',"
                                + " '2019-01-01T12:12:12',"
                                + " '435463424', 'false', '1.2', '1.0', '11:12:12', 'POINT(1 3)')")
                .await();
    }

    /** sink Nebula Graph Edge Data with default INSERT mode */
    @Test
    public void sinkEdgeDataWithoutRank() throws ExecutionException, InterruptedException {
        TableEnvironment tableEnvironment =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        tableEnvironment.executeSql(
                "CREATE TABLE friend ("
                        + "src STRING,"
                        + "dst STRING,"
                        + "col1 STRING,"
                        + "col2 STRING,"
                        + "col3 STRING,"
                        + "col4 STRING,"
                        + "col5 STRING,"
                        + "col6 STRING,"
                        + "col7 STRING,"
                        + "col8 STRING,"
                        + "col9 STRING,"
                        + "col10 STRING,"
                        + "col11 STRING,"
                        + "col12 STRING,"
                        + "col13 STRING,"
                        + "col14 STRING"
                        + ")"
                        + "WITH ("
                        + "'connector'='nebula',"
                        + "'meta-address'='"
                        + META_ADDRESS
                        + "',"
                        + "'graph-address'='"
                        + GRAPH_ADDRESS
                        + "',"
                        + "'username'='"
                        + USER_NAME
                        + "',"
                        + "'password'='"
                        + PASSWORD
                        + "',"
                        + "'graph-space'='flinkSink',"
                        + "'src-index'='0',"
                        + "'dst-index'='1',"
                        + "'write-mode'='insert',"
                        + "'data-type'='edge'"
                        + ")");

        tableEnvironment
                .executeSql(
                        "insert into friend values ('61', '89', 'aba', 'abcdefgh',"
                                + " '1', '1111', '22222', '6412233', '2019-01-01',"
                                + " '2019-01-01T12:12:12', '435463424', 'false', '1.2', '1.0',"
                                + " '11:12:12', 'POINT(1 3)')")
                .await();
    }
}
