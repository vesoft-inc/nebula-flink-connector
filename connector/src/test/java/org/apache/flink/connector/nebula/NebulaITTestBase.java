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

public class NebulaITTestBase {

    protected static final String META_ADDRESS = "127.0.0.1:9559";
    protected static final String GRAPH_ADDRESS = "127.0.0.1:9669";
    protected static final String USERNAME = "root";
    protected static final String PASSWORD = "nebula";

    protected static Session session;
    protected static NebulaPool pool;

    protected static void initializeNebulaSession() {
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
            throw new RuntimeException("init nebula pool error", e);
        }
        try {
            session = pool.getSession(USERNAME, PASSWORD, true);
        } catch (NotValidConnectionException
                 | AuthFailedException
                 | IOErrorException
                 | ClientServerIncompatibleException e) {
            throw new RuntimeException("init nebula session error", e);
        }
    }

    protected static void initializeNebulaSchema(String statement) {
        executeNGql(statement);
        // wait for at least two heartbeat cycles
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    protected static void closeNebulaSession() {
        if (session != null) {
            session.release();
        }
        if (pool != null) {
            pool.close();
        }
    }

    protected static ResultSet executeNGql(String stmt) {
        ResultSet response;
        try {
            response = session.execute(stmt);
        } catch (IOErrorException e) {
            throw new RuntimeException(String.format("failed to execute statement %s", stmt), e);
        }
        if (!response.isSucceeded()) {
            throw new RuntimeException(String.format(
                    "failed to execute statement %s with error: %s",
                    stmt, response.getErrorMessage()));
        }
        return response;
    }

    protected static void check(List<Row> expected, String stmt) {
        ResultSet response = executeNGql(stmt);
        if (expected == null || expected.isEmpty()) {
            assertTrue(response.isEmpty());
        } else {
            assertEquals(expected, response.getRows());
        }
    }
}
