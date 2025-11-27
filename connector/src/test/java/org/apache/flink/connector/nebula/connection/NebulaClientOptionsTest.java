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
import org.junit.Test;

public class NebulaClientOptionsTest {
    @Test
    public void testConfigAddress() {
        ConnectionOptions.builder()
                .withGraphAddress(graphAddr)
                .withUser(user)
                .withPassword(passwd)
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMetaAddressWithEmptyAddress() {
        ConnectionOptions.builder()
                .withGraphAddress(null)
                .withUser(user)
                .withPassword(passwd)
                .build();
    }

}
