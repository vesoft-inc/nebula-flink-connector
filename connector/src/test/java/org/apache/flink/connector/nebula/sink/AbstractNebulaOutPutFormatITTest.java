/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.sink;

import com.vesoft.nebula.client.graph.NebulaPoolConfig;
import com.vesoft.nebula.client.graph.data.HostAddress;
import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.exception.AuthFailedException;
import com.vesoft.nebula.client.graph.exception.ClientServerIncompatibleException;
import com.vesoft.nebula.client.graph.exception.IOErrorException;
import com.vesoft.nebula.client.graph.exception.NotValidConnectionException;
import com.vesoft.nebula.client.graph.net.NebulaPool;
import com.vesoft.nebula.client.graph.net.Session;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AbstractNebulaOutPutFormatITTest {
    static final String META_ADDRESS = "127.0.0.1:9559";
    static final String GRAPH_ADDRESS = "127.0.0.1:9669";
    static final String USER_NAME = "root";
    static final String PASSWORD = "nebula";
    private static final Logger LOGGER =
            LoggerFactory.getLogger(AbstractNebulaOutPutFormatITTest.class);

    @Before
    public void mockData() {
        NebulaPoolConfig nebulaPoolConfig = new NebulaPoolConfig();
        List<HostAddress> addresses = Arrays.asList(new HostAddress("127.0.0.1", 9669));
        NebulaPool pool = new NebulaPool();
        Session session = null;
        try {
            pool.init(addresses, nebulaPoolConfig);
            session = pool.getSession("root", "nebula", true);

            ResultSet respFlinkSinkSpace = session.execute(createFlinkSinkSpace());
            ResultSet respTagPerson = session.execute(createTagPerson());

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            if (!respFlinkSinkSpace.isSucceeded()) {
                LOGGER.error(
                        "create flinkSink vid type space failed, {}",
                        respFlinkSinkSpace.getErrorMessage());
                assert (false);
            }

            if (!respTagPerson.isSucceeded()) {
                LOGGER.error("create tag person failed, {}", respTagPerson.getErrorMessage());
                assert (false);
            }

            ResultSet respEdgeFriend = session.execute(createEdgeFriendPerson());
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            if (!respEdgeFriend.isSucceeded()) {
                LOGGER.error("create edge friend failed, {}", respTagPerson.getErrorMessage());
                assert (false);
            }
        } catch (UnknownHostException
                | NotValidConnectionException
                | IOErrorException
                | AuthFailedException
                | ClientServerIncompatibleException e) {
            LOGGER.error("create space error, ", e);
            assert (false);
        } finally {
            pool.close();
        }
    }

    private String createFlinkSinkSpace() {
        return "CREATE SPACE IF NOT EXISTS `flinkSink` (partition_num = 100, charset = utf8,"
                + " collate = utf8_bin, vid_type = INT64);"
                + "USE `flinkSink`;";
    }

    private String createTagPerson() {
        return "CREATE TAG IF NOT EXISTS person(col1 string, col2 fixed_string(8), col3 int8,"
                + " col4 int16,"
                + " col5 int32, col6 int64, col7 date, col8 datetime, col9 timestamp, col10 bool,"
                + " col11 double,"
                + " col12 float, col13 time, col14 geography);";
    }

    private String createEdgeFriendPerson() {
        return "CREATE EDGE IF NOT EXISTS friend(col1 string, col2 fixed_string(8), col3 int8,"
                + " col4 int16,"
                + " col5 int32, col6 int64, col7 date, col8 datetime, col9 timestamp, col10 bool,"
                + " col11 double,"
                + " col12 float, col13 time, col14 geography);";
    }

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
                        + "'src-id-index'='0',"
                        + "'dst-id-index'='1',"
                        + "'rank-id-index'='4',"
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
                        + "'src-id-index'='0',"
                        + "'dst-id-index'='1',"
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
