
/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.connection;

import org.apache.flink.connector.nebula.utils.SSLSignType;
import org.junit.Test;

public class NebulaClientOptionsTest {
    @Test
    public void testConfigAddress() {
        NebulaClientOptions nebulaClientOptions =
                new NebulaClientOptions.NebulaClientOptionsBuilder()
                        .setGraphAddress("127.0.0.1:9669")
                        .setMetaAddress("127.0.0.1:9559")
                        .setUsername("root")
                        .setPassword("nebula")
                        .setConnectRetry(1)
                        .setTimeout(1000)
                        .setEnableGraphSSL(true)
                        .setEnableMetaSSL(true)
                        .setSSLSignType(SSLSignType.CA)
                        .setCaSignParam("caCrtFile", "crtFile", "keyFile")
                        .setSelfSignParam("crtFile", "keyFile", "password")
                        .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMetaAddressWithEmptyAddress() {
        new NebulaClientOptions.NebulaClientOptionsBuilder()
                .setGraphAddress("127.0.0.1:9669")
                .setMetaAddress(null)
                .build();
    }

    @Test
    public void testGraphAddressWithEmptyAddress() {
        new NebulaClientOptions.NebulaClientOptionsBuilder()
                .setGraphAddress(null)
                .setMetaAddress("127.0.0.1:9559")
                .build();
    }


    @Test(expected = IllegalArgumentException.class)
    public void testIsEnableGraphSsl() {
        NebulaClientOptions nebulaClientOptions = new NebulaClientOptions
                .NebulaClientOptionsBuilder()
                .setGraphAddress(null)
                .setMetaAddress("127.0.0.1:9559")
                .setEnableMetaSSL(false)
                .setEnableStorageSSL(true)
                .setSSLSignType(SSLSignType.CA)
                .setCaSignParam("caCrtFile", "crtFile", "keyFile")
                .build();
    }


    @Test(expected = IllegalArgumentException.class)
    public void testNullSslSignType() {
        NebulaClientOptions nebulaClientOptions = new NebulaClientOptions
                .NebulaClientOptionsBuilder()
                .setGraphAddress(null)
                .setMetaAddress("127.0.0.1:9559")
                .setEnableGraphSSL(false)
                .setEnableMetaSSL(true)
                .setSSLSignType(null)
                .setCaSignParam("caCrtFile", "crtFile", "keyFile")
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNoSslSignType() {
        NebulaClientOptions nebulaClientOptions = new NebulaClientOptions
                .NebulaClientOptionsBuilder()
                .setGraphAddress(null)
                .setMetaAddress("127.0.0.1:9559")
                .setEnableGraphSSL(false)
                .setEnableMetaSSL(true)
                .setCaSignParam("caCrtFile", "crtFile", "keyFile")
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetCaSignParam() {
        NebulaClientOptions nebulaClientOptions = new NebulaClientOptions
                .NebulaClientOptionsBuilder()
                .setGraphAddress(null)
                .setMetaAddress("127.0.0.1:9559")
                .setEnableGraphSSL(false)
                .setEnableMetaSSL(true)
                .setSSLSignType(SSLSignType.CA)
                .setSelfSignParam("crtFile", "keyFile", "password")
                .build();

    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetSelfSignParam() {
        NebulaClientOptions nebulaClientOptions = new NebulaClientOptions
                .NebulaClientOptionsBuilder()
                .setGraphAddress(null)
                .setMetaAddress("127.0.0.1:9559")
                .setEnableGraphSSL(false)
                .setEnableMetaSSL(true)
                .setSSLSignType(SSLSignType.SELF)
                .setCaSignParam("caCrtFile", "crtFile", "keyFile")
                .build();
    }
}
