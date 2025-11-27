/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula;

import static org.apache.flink.connector.nebula.TestConstant.graphAddr;
import static org.apache.flink.connector.nebula.TestConstant.passwd;
import static org.apache.flink.connector.nebula.TestConstant.user;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.vesoft.nebula.driver.graph.data.ResultSet;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.connector.nebula.connection.GraphProvider;
import org.apache.flink.connector.nebula.options.ConnectionOptions;

public class NebulaITTestBase {

    protected static GraphProvider graphProvider;

    protected static void initializeNebulaClient() {

        try {
            ConnectionOptions connectionOptions = ConnectionOptions
                    .builder()
                    .withGraphAddress(graphAddr)
                    .withUser(user)
                    .withPassword(passwd)
                    .withZonedDatetimeFormat("%Y-%m-%dT%H:%M:%S %Ez")
                    .withZonedTimeFormat("%H:%M:%S %Ez")
                    .build();
            graphProvider = new GraphProvider(connectionOptions);
        } catch (Exception e) {
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

    protected static void closeGraphProvider() {
        if (graphProvider != null) {
            graphProvider.close();
        }
    }

    protected static ResultSet executeNGql(String stmt) {
        ResultSet response;
        try {
            response = graphProvider.execute(stmt);
        } catch (Exception e) {
            throw new RuntimeException(String.format("failed to execute statement %s", stmt), e);
        }
        if (!response.isSucceeded()) {
            throw new RuntimeException(String.format(
                    "failed to execute statement %s with error: %s",
                    stmt, response.getErrorMessage()));
        }
        return response;
    }

    protected static void check(List<ResultSet.Record> expected, String stmt) {
        ResultSet response = executeNGql(stmt);
        if (expected == null || expected.isEmpty()) {
            assertTrue(response.isEmpty());
        } else {
            List<ResultSet.Record> result = new ArrayList<>();
            while (response.hasNext()) {
                result.add(response.next());
            }
            assertEquals(expected, result);
        }
    }
}
