/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.source;

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
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * vertex as source: vid must be included in CREATE & SELECT edge as source: srcId, dstId, rank must
 * be included in CREATE & SELECT
 */
public class NebulaInputFormatITTest {
    static final String META_ADDRESS = "127.0.0.1:9559";
    static final String GRAPH_ADDRESS = "127.0.0.1:9669";
    static final String USER_NAME = "root";
    static final String PASSWORD = "nebula";
    private static final Logger LOGGER = LoggerFactory.getLogger(NebulaInputFormatITTest.class);

    @Before
    public void mockData() {
        NebulaPoolConfig nebulaPoolConfig = new NebulaPoolConfig();
        List<HostAddress> addresses = Arrays.asList(new HostAddress("127.0.0.1", 9669));
        NebulaPool pool = new NebulaPool();
        Session session;
        try {
            pool.init(addresses, nebulaPoolConfig);
            session = pool.getSession("root", "nebula", true);

            ResultSet respFlinkSourceSpace = session.execute(createSpace());
            ResultSet respTagAndEdge = session.execute(createTagAndEdge());

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            if (!respFlinkSourceSpace.isSucceeded()) {
                LOGGER.error(
                        "create flinkSource vid type space failed, {}",
                        respFlinkSourceSpace.getErrorMessage());
                assert (false);
            }

            if (!respTagAndEdge.isSucceeded()) {
                LOGGER.error("create tag failed, {}", respTagAndEdge.getErrorMessage());
                assert (false);
            }

            ResultSet respInsert = session.execute(insertData());
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            if (!respInsert.isSucceeded()) {
                LOGGER.error("insert failed, {}", respInsert.getErrorMessage());
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

    private String insertData() {
        return "INSERT VERTEX player(name, age) VALUES \"player100\":(\"Tim Duncan\", 42);"
                + "INSERT VERTEX player(name, age) VALUES \"player101\":(\"Tony Parker\", 36);"
                + "INSERT VERTEX player(name, age) VALUES "
                + "\"player102\":(\"LaMarcus Aldridge\", 33);"
                + "INSERT VERTEX team(name) VALUES "
                + "\"team203\":(\"Trail Blazers\"),\"team204\":(\"Spurs\");"
                + "INSERT EDGE follow(degree) VALUES \"player101\" -> \"player100\":(95);"
                + "INSERT EDGE follow(degree) VALUES \"player101\" -> \"player102\":(90);"
                + "INSERT EDGE follow(degree) VALUES \"player102\" -> \"player100\":(75);"
                + "INSERT EDGE serve(start_year, end_year) VALUES \"player101\" -> \"team204\":"
                + "(1999, 2018),\"player102\" -> \"team203\":(2006,  2015);";
    }

    private String createTagAndEdge() {
        return "CREATE TAG IF NOT EXISTS player(name string, age int);"
                + "CREATE TAG IF NOT EXISTS team(name string);"
                + "CREATE EDGE IF NOT EXISTS follow(degree int);"
                + "CREATE EDGE IF NOT EXISTS serve(start_year int, end_year int);";
    }

    private String createSpace() {
        return "CREATE SPACE IF NOT EXISTS basketballplayer(partition_num=1, replica_factor=1,"
                + " vid_type=fixed_string(30));USE basketballplayer;";
    }

    @Test
    public void testReadVertexAsStreamWithAllColumns() {
        TableEnvironment tableEnvironment =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        tableEnvironment.executeSql(
                "CREATE TABLE player ("
                        + "vid STRING,"
                        + "name STRING,"
                        + "age BIGINT"
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
                        + "'graph-space'='basketballplayer',"
                        + "'data-type'='vertex'"
                        + ")");

        tableEnvironment.executeSql("select vid, name, age from player").print();
    }

    @Test
    public void testReadVertexAsStreamWith2Columns() {
        TableEnvironment tableEnvironment =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        tableEnvironment.executeSql(
                "CREATE TABLE player ("
                        + "vid STRING,"
                        + "name STRING"
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
                        + "'graph-space'='basketballplayer',"
                        + "'data-type'='vertex'"
                        + ")");

        tableEnvironment.executeSql("select vid, name from player").print();
    }

    @Test
    public void testReadEdgeAsStreamWithAllColumns() {
        TableEnvironment tableEnvironment =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        tableEnvironment.executeSql(
                "CREATE TABLE follow ("
                        + "srcId STRING,"
                        + "dstId STRING,"
                        + "`rank` BIGINT,"
                        + "src STRING,"
                        + "dst STRING,"
                        + "degree BIGINT"
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
                        + "'graph-space'='basketballplayer',"
                        + "'data-type'='edge'"
                        + ")");

        tableEnvironment
                .executeSql("select srcId, dstId, `rank`, src, dst, degree from follow")
                .print();
    }

    @Test
    public void testReadEdgeAsStreamWith2Columns() {
        TableEnvironment tableEnvironment =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        tableEnvironment.executeSql(
                "CREATE TABLE serve ("
                        + "srcId STRING,"
                        + "dstId STRING,"
                        + "`rank` BIGINT,"
                        + "src STRING,"
                        + "dst STRING"
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
                        + "'graph-space'='basketballplayer',"
                        + "'data-type'='edge'"
                        + ")");

        tableEnvironment.executeSql("select srcId, dstId, `rank`, src, dst from serve").print();
    }
}
