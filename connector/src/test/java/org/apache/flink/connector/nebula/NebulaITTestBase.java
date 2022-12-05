/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.vesoft.nebula.Row;
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
import java.util.Collections;
import java.util.List;
import org.apache.flink.connector.nebula.utils.NebulaConstant;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class NebulaITTestBase {

    protected static final String META_ADDRESS = "127.0.0.1:9559";
    protected static final String GRAPH_ADDRESS = "127.0.0.1:9669";
    protected static final String USER_NAME = "root";
    protected static final String PASSWORD = "nebula";

    private static final String[] NEBULA_SCHEMA_STATEMENTS = new String[]{
        "CLEAR SPACE IF EXISTS `flink_test`;"
                + " CREATE SPACE IF NOT EXISTS `flink_test` (partition_num = 100,"
                + " charset = utf8, replica_factor = 3, collate = utf8_bin, vid_type = INT64);"
                + " USE `flink_test`;",
        "CREATE TAG IF NOT EXISTS person (col1 string, col2 fixed_string(8),"
                + " col3 int8, col4 int16, col5 int32, col6 int64,"
                + " col7 date, col8 datetime, col9 timestamp, col10 bool,"
                + " col11 double, col12 float, col13 time, col14 geography);"
                + " CREATE EDGE IF NOT EXISTS friend (col1 string, col2 fixed_string(8),"
                + " col3 int8, col4 int16, col5 int32, col6 int64,"
                + " col7 date, col8 datetime, col9 timestamp, col10 bool,"
                + " col11 double, col12 float, col13 time, col14 geography);"
    };
    protected static Session session;
    protected static NebulaPool pool;
    protected static TableEnvironment tableEnvironment;

    @BeforeClass
    public static void beforeAll() throws IOErrorException {
        initializeNebulaSession();
        initializeNebulaSchema();
    }

    @AfterClass
    public static void afterAll() {
        closeNebulaSession();
    }

    private static void initializeNebulaSession() throws IOErrorException {
        NebulaPoolConfig nebulaPoolConfig = new NebulaPoolConfig();
        String[] addressAndPort = GRAPH_ADDRESS.split(NebulaConstant.COLON);
        List<HostAddress> addresses = Collections.singletonList(
                new HostAddress(addressAndPort[0], Integer.parseInt(addressAndPort[1]))
        );
        pool = new NebulaPool();
        try {
            boolean result = pool.init(addresses, nebulaPoolConfig);
            if (!result) {
                throw new RuntimeException("failed to initialize connection pool");
            }
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
        try {
            session = pool.getSession(USER_NAME, PASSWORD, true);
        } catch (NotValidConnectionException
                 | AuthFailedException
                 | ClientServerIncompatibleException e) {
            throw new RuntimeException(e);
        }
    }

    private static void initializeNebulaSchema() throws IOErrorException {
        for (String stmt : NEBULA_SCHEMA_STATEMENTS) {
            executeNGql(stmt);
            // wait for at least two heartbeat cycles
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static void closeNebulaSession() {
        if (session != null) {
            session.release();
        }
        if (pool != null) {
            pool.close();
        }
    }

    @Before
    public void before() {
        tableEnvironment = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    }

    protected static ResultSet executeNGql(String stmt) throws IOErrorException {
        ResultSet response = session.execute(stmt);
        if (!response.isSucceeded()) {
            throw new RuntimeException(String.format(
                    "failed to execute statement %s with error: %s",
                    stmt, response.getErrorMessage()));
        }
        return response;
    }

    protected static void check(List<Row> expected, String stmt) {
        ResultSet response;
        try {
            response = executeNGql(stmt);
        } catch (IOErrorException e) {
            throw new RuntimeException(String.format("failed to check result of %s", stmt), e);
        }
        if (expected == null || expected.isEmpty()) {
            assertTrue(response.isEmpty());
        } else {
            assertEquals(expected, response.getRows());
        }
    }
}
