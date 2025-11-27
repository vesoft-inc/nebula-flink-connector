/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.connection;

import static org.apache.flink.connector.nebula.TestConstant.graphAddr;
import static org.apache.flink.connector.nebula.TestConstant.passwd;
import static org.apache.flink.connector.nebula.TestConstant.user;

import org.apache.flink.connector.nebula.options.ConnectionOptions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphProviderTest {
    private static final Logger LOG =
            LoggerFactory.getLogger(GraphProviderTest.class);

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void getNebulaPool() {
        ConnectionOptions connectionOptions =
                ConnectionOptions.builder()
                        .withGraphAddress(graphAddr)
                        .withUser(user)
                        .withPassword(passwd)
                        .build();
        GraphProvider graphProvider = null;
        try {
            graphProvider = new GraphProvider(connectionOptions);
        } catch (Exception e) {
            LOG.info("create graphProvider failed, ", e);
            assert (false);
        } finally {
            if (graphProvider != null) {
                graphProvider.close();
            }
        }
    }
}
