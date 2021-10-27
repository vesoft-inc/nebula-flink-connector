/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package org.apache.flink.connector.nebula.connection;

import com.vesoft.nebula.client.graph.exception.AuthFailedException;
import com.vesoft.nebula.client.graph.exception.ClientServerIncompatibleException;
import com.vesoft.nebula.client.graph.exception.IOErrorException;
import com.vesoft.nebula.client.graph.exception.NotValidConnectionException;
import com.vesoft.nebula.client.graph.net.NebulaPool;
import java.net.UnknownHostException;
import org.apache.flink.connector.nebula.utils.SSLSighType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NebulaGraphConnectionProviderTest {
    private static final Logger LOG =
            LoggerFactory.getLogger(NebulaGraphConnectionProviderTest.class);

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void getNebulaPool() {
        NebulaClientOptions nebulaClientOptions =
                new NebulaClientOptions.NebulaClientOptionsBuilder()
                        .setGraphAddress("127.0.0.1:9669")
                        .setMetaAddress("127.0.0.1:9559")
                        .setUsername("root")
                        .setPassword("nebula")
                        .setConnectRetry(1)
                        .setTimeout(1000)
                        .build();
        NebulaGraphConnectionProvider graphConnectionProvider =
                new NebulaGraphConnectionProvider(nebulaClientOptions);
        try {
            NebulaPool nebulaPool = graphConnectionProvider.getNebulaPool();
            nebulaPool.getSession("root", "nebula", true);
        } catch (Exception e) {
            LOG.info("get session failed, ", e);
            assert (false);
        }
    }

    /**
     * nebula server does not enable ssl, the connect cannot be established correctly.
     */
    @Test(expected = NotValidConnectionException.class)
    public void getSessionWithSsl() throws NotValidConnectionException {
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
                        .setSSLSignType(SSLSighType.CA)
                        .setCaSignParam("src/test/resources/ssl/casigned.pem",
                                "src/test/resources/ssl/casigned.crt",
                                "src/test/resources/ssl/casigned.key")
                        .build();


        NebulaGraphConnectionProvider graphConnectionProvider =
                new NebulaGraphConnectionProvider(nebulaClientOptions);

        try {
            NebulaPool pool = graphConnectionProvider.getNebulaPool();
            pool.getSession("root", "nebula", true);
        } catch (UnknownHostException | IOErrorException | AuthFailedException
                | ClientServerIncompatibleException e) {
            LOG.error("get session faied, ", e);
            assert (false);
        }
    }

    // todo test ssl for server
}
