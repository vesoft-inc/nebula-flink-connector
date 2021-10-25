package org.apache.flink.connector.nebula.connection;

import com.vesoft.nebula.client.graph.exception.AuthFailedException;
import com.vesoft.nebula.client.graph.exception.ClientServerIncompatibleException;
import com.vesoft.nebula.client.graph.exception.IOErrorException;
import com.vesoft.nebula.client.graph.exception.NotValidConnectionException;
import org.apache.flink.connector.nebula.utils.SslSighType;
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
    public void getSession() {
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
            graphConnectionProvider.getSession();
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
                        .setEnableGraphSsl(true)
                        .setEnableMetaSsl(true)
                        .setSslSignType(SslSighType.CA)
                        .setCaSignParam("src/test/resources/ssl/casigned.pem",
                                "src/test/resources/ssl/casigned.crt",
                                "src/test/resources/ssl/casigned.key")
                        .build();


        NebulaGraphConnectionProvider graphConnectionProvider =
                new NebulaGraphConnectionProvider(nebulaClientOptions);

        try {
            graphConnectionProvider.getSession();
        } catch (IOErrorException | AuthFailedException | ClientServerIncompatibleException e) {
            LOG.error("get session faied, ", e);
            assert (false);
        }
    }

    // todo test ssl for server
}
