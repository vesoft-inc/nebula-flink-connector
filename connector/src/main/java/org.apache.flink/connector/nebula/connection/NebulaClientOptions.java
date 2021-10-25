/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package org.apache.flink.connector.nebula.connection;

import com.vesoft.nebula.client.graph.data.CASignedSSLParam;
import com.vesoft.nebula.client.graph.data.HostAddress;
import com.vesoft.nebula.client.graph.data.SelfSignedSSLParam;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.connector.nebula.utils.NebulaConstant;
import org.apache.flink.connector.nebula.utils.SslSighType;

public class NebulaClientOptions implements Serializable {

    private static final long serialVersionUID = 5685521189643221375L;

    private final String metaAddress;

    private final String graphAddress;

    private final String username;

    private final String password;

    private final int timeout;

    private final int connectRetry;

    private final boolean enableGraphSsl;

    private final boolean enableMetaSsl;

    private final SslSighType sslSighType;

    private final CASignedSSLParam caSignParam;

    private final SelfSignedSSLParam selfSignParam;


    private NebulaClientOptions(String metaAddress, String graphAddress, String username,
                                String password, int timeout, int connectRetry,
                                boolean enableGraphSsl, boolean enableMetaSsl,
                                SslSighType sslSighType, CASignedSSLParam caSignParam,
                                SelfSignedSSLParam selfSignParam) {
        this.metaAddress = metaAddress;
        this.graphAddress = graphAddress;
        this.username = username;
        this.password = password;
        this.timeout = timeout;
        this.connectRetry = connectRetry;
        this.enableGraphSsl = enableGraphSsl;
        this.enableMetaSsl = enableMetaSsl;
        this.sslSighType = sslSighType;
        this.caSignParam = caSignParam;
        this.selfSignParam = selfSignParam;
    }

    public List<HostAddress> getMetaAddress() {
        List<HostAddress> addresses = new ArrayList<>();
        for (String address : metaAddress.split(NebulaConstant.COMMA)) {
            String[] hostAndPort = address.split(NebulaConstant.COLON);
            addresses.add(new HostAddress(hostAndPort[0], Integer.parseInt(hostAndPort[1])));
        }
        return addresses;
    }

    public String getGraphAddress() {
        return graphAddress;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public int getTimeout() {
        return timeout;
    }

    public int getConnectRetry() {
        return connectRetry;
    }

    public boolean isEnableGraphSsl() {
        return enableGraphSsl;
    }

    public boolean isEnableMetaSsl() {
        return enableMetaSsl;
    }

    public SslSighType getSslSighType() {
        return sslSighType;
    }

    public CASignedSSLParam getCaSignParam() {
        return caSignParam;
    }

    public SelfSignedSSLParam getSelfSignParam() {
        return selfSignParam;
    }

    /**
     * Builder for {@link NebulaClientOptions}
     */
    public static class NebulaClientOptionsBuilder {
        private String metaAddress;
        private String graphAddress;
        private String username = "root";
        private String password = "nebula";
        private int timeout = 6000;
        private int connectRetry = 1;

        // ssl options
        private boolean enableGraphSsl = false;
        private boolean enableMetaSsl = false;
        private SslSighType sslSighType = null;
        private CASignedSSLParam caSignParam = null;
        private SelfSignedSSLParam selfSignParam = null;

        public NebulaClientOptionsBuilder setMetaAddress(String metaAddress) {
            this.metaAddress = metaAddress;
            return this;
        }

        public NebulaClientOptionsBuilder setGraphAddress(String graphAddress) {
            this.graphAddress = graphAddress;
            return this;
        }

        public NebulaClientOptionsBuilder setUsername(String username) {
            this.username = username;
            return this;
        }

        public NebulaClientOptionsBuilder setPassword(String password) {
            this.password = password;
            return this;
        }

        public NebulaClientOptionsBuilder setTimeout(int timeout) {
            this.timeout = timeout;
            return this;
        }

        public NebulaClientOptionsBuilder setConnectRetry(int connectRetry) {
            this.connectRetry = connectRetry;
            return this;
        }

        public NebulaClientOptionsBuilder setEnableGraphSsl(boolean enableGraphSsl) {
            this.enableGraphSsl = enableGraphSsl;
            return this;
        }

        public NebulaClientOptionsBuilder setEnableMetaSsl(boolean enableMetaSsl) {
            this.enableMetaSsl = enableMetaSsl;
            return this;
        }

        public NebulaClientOptionsBuilder setSslSignType(SslSighType sslSighType) {
            this.sslSighType = sslSighType;
            return this;
        }

        public NebulaClientOptionsBuilder setCaSignParam(String caCrtFilePath, String crtFilePath,
                                                         String keyFilePath) {
            this.caSignParam = new CASignedSSLParam(caCrtFilePath, crtFilePath,
                    keyFilePath);
            return this;
        }

        public NebulaClientOptionsBuilder setSelfSignParam(String crtFilePath, String keyFilePath,
                                                           String password) {
            this.selfSignParam = new SelfSignedSSLParam(crtFilePath, keyFilePath, password);
            return this;
        }

        public NebulaClientOptions build() {
            if (metaAddress == null || metaAddress.trim().isEmpty()) {
                throw new IllegalArgumentException("meta address can not be empty.");
            }
            if (enableMetaSsl || enableGraphSsl) {
                // if meta is set to open ssl, then graph must be set to open ssl
                enableGraphSsl = true;
                if (sslSighType == null) {
                    throw new IllegalArgumentException("ssl is enable, ssl sign type must not be "
                            + "null");
                }
                switch (sslSighType) {
                    case CA:
                        if (caSignParam == null) {
                            throw new IllegalArgumentException("ssl is enable and sign type is "
                                    + "CA, caSignParam must not be null");
                        }
                        break;
                    case SELF:
                        if (selfSignParam == null) {
                            throw new IllegalArgumentException("ssl is enable and sign type is "
                                    + "CA, selfSignParam must not be null");
                        }
                        break;
                    default:
                        throw new IllegalArgumentException("ssl sign type is not supported.");
                }

            }

            return new NebulaClientOptions(
                    metaAddress,
                    graphAddress,
                    username,
                    password,
                    timeout,
                    connectRetry,
                    enableGraphSsl,
                    enableMetaSsl,
                    sslSighType,
                    caSignParam,
                    selfSignParam);
        }
    }
}
