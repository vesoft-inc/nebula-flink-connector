package org.apache.flink.connector.nebula.connection;


import org.apache.flink.connector.nebula.utils.SslSighType;
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
                        .setEnableGraphSsl(true)
                        .setEnableMetaSsl(true)
                        .setSslSignType(SslSighType.CA)
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


    @Test
    public void testIsEnableGraphSsl() {
        NebulaClientOptions nebulaClientOptions = new NebulaClientOptions
                .NebulaClientOptionsBuilder()
                .setGraphAddress(null)
                .setMetaAddress("127.0.0.1:9559")
                .setEnableGraphSsl(false)
                .setEnableMetaSsl(true)
                .setSslSignType(SslSighType.CA)
                .setCaSignParam("caCrtFile", "crtFile", "keyFile")
                .build();
        assert (nebulaClientOptions.isEnableGraphSsl());
    }


    @Test(expected = IllegalArgumentException.class)
    public void testNullSslSighType() {
        NebulaClientOptions nebulaClientOptions = new NebulaClientOptions
                .NebulaClientOptionsBuilder()
                .setGraphAddress(null)
                .setMetaAddress("127.0.0.1:9559")
                .setEnableGraphSsl(false)
                .setEnableMetaSsl(true)
                .setSslSignType(null)
                .setCaSignParam("caCrtFile", "crtFile", "keyFile")
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNoSslSighType() {
        NebulaClientOptions nebulaClientOptions = new NebulaClientOptions
                .NebulaClientOptionsBuilder()
                .setGraphAddress(null)
                .setMetaAddress("127.0.0.1:9559")
                .setEnableGraphSsl(false)
                .setEnableMetaSsl(true)
                .setCaSignParam("caCrtFile", "crtFile", "keyFile")
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetCaSignParam() {
        NebulaClientOptions nebulaClientOptions = new NebulaClientOptions
                .NebulaClientOptionsBuilder()
                .setGraphAddress(null)
                .setMetaAddress("127.0.0.1:9559")
                .setEnableGraphSsl(false)
                .setEnableMetaSsl(true)
                .setSslSignType(SslSighType.CA)
                .setSelfSignParam("crtFile", "keyFile", "password")
                .build();

    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetSelfSignParam() {
        NebulaClientOptions nebulaClientOptions = new NebulaClientOptions
                .NebulaClientOptionsBuilder()
                .setGraphAddress(null)
                .setMetaAddress("127.0.0.1:9559")
                .setEnableGraphSsl(false)
                .setEnableMetaSsl(true)
                .setSslSignType(SslSighType.SELF)
                .setCaSignParam("caCrtFile", "crtFile", "keyFile")
                .build();
    }
}
