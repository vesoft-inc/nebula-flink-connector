/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.connection;

import com.vesoft.nebula.client.graph.data.HostAddress;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.connector.nebula.utils.NebulaConstant;
import org.apache.flink.connector.nebula.utils.SSLSignType;

public class NebulaClientOptions implements Serializable {

    private static final long serialVersionUID = 5685521189643221375L;

    private final String metaAddress;

    private final String graphAddress;

    private final String username;

    private final String password;

    private final int timeout;

    private final int connectRetry;

    private final boolean enableGraphSSL;

    private final boolean enableMetaSSL;

    private final boolean enableStorageSSL;

    private final SSLSignType sslSignType;

    private final CASignParams caSignParams;

    private final SelfSignParams selfSignParams;

    private final String handshakeKey;

    private NebulaClientOptions(String metaAddress, String graphAddress, String username,
                                String password, int timeout, int connectRetry,
                                boolean enableGraphSSL, boolean enableMetaSSL,
                                boolean enableStorageSSL,
                                SSLSignType sslSignType, CASignParams caSignParams,
                                SelfSignParams selfSignParams, String handshakeKey) {
        this.metaAddress = metaAddress;
        this.graphAddress = graphAddress;
        this.username = username;
        this.password = password;
        this.timeout = timeout;
        this.connectRetry = connectRetry;
        this.enableGraphSSL = enableGraphSSL;
        this.enableMetaSSL = enableMetaSSL;
        this.enableStorageSSL = enableStorageSSL;
        this.sslSignType = sslSignType;
        this.caSignParams = caSignParams;
        this.selfSignParams = selfSignParams;
        this.handshakeKey = handshakeKey;
    }

    public List<HostAddress> getMetaAddress() {
        List<HostAddress> addresses = new ArrayList<>();
        for (String address : metaAddress.split(NebulaConstant.COMMA)) {
            String[] hostAndPort = address.split(NebulaConstant.COLON);
            addresses.add(new HostAddress(hostAndPort[0], Integer.parseInt(hostAndPort[1])));
        }
        return addresses;
    }

    public String getRawMetaAddress() {
        return metaAddress;
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

    public boolean isEnableGraphSSL() {
        return enableGraphSSL;
    }

    public boolean isEnableMetaSSL() {
        return enableMetaSSL;
    }

    public boolean isEnableStorageSSL() {
        return enableStorageSSL;
    }

    public SSLSignType getSSLSignType() {
        return sslSignType;
    }

    public CASignParams getCaSignParam() {
        return caSignParams;
    }

    public SelfSignParams getSelfSignParam() {
        return selfSignParams;
    }

    public String getHandshakeKey() {
        return handshakeKey;
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
        private boolean enableGraphSSL = false;
        private boolean enableMetaSSL = false;
        private boolean enableStorageSSL = false;
        private SSLSignType sslSignType = null;
        private CASignParams caSignParams = null;
        private SelfSignParams selfSignParams = null;
        private String handshakeKey = null;

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

        public NebulaClientOptionsBuilder setEnableGraphSSL(boolean enableGraphSSL) {
            this.enableGraphSSL = enableGraphSSL;
            return this;
        }

        public NebulaClientOptionsBuilder setEnableMetaSSL(boolean enableMetaSSL) {
            this.enableMetaSSL = enableMetaSSL;
            return this;
        }

        public NebulaClientOptionsBuilder setEnableStorageSSL(boolean enableStorageSSL) {
            this.enableStorageSSL = enableStorageSSL;
            return this;
        }


        public NebulaClientOptionsBuilder setSSLSignType(SSLSignType sslSignType) {
            this.sslSignType = sslSignType;
            return this;
        }

        public NebulaClientOptionsBuilder setCaSignParam(String caCrtFilePath, String crtFilePath,
                                                         String keyFilePath) {
            this.caSignParams = new CASignParams(caCrtFilePath, crtFilePath, keyFilePath);
            return this;
        }

        public NebulaClientOptionsBuilder setSelfSignParam(String crtFilePath, String keyFilePath,
                                                           String password) {
            this.selfSignParams = new SelfSignParams(crtFilePath, keyFilePath, password);
            return this;
        }

        public NebulaClientOptionsBuilder setHandshakeKey(String handshakeKey) {
            this.handshakeKey = handshakeKey;
            return this;
        }

        public NebulaClientOptions build() {
            if (metaAddress == null || metaAddress.trim().isEmpty()) {
                throw new IllegalArgumentException("meta address can not be empty.");
            }
            if (enableMetaSSL || enableGraphSSL || enableStorageSSL) {
                // if storage is set to open ssl, then meta must be set to open ssl
                if (enableStorageSSL && !enableMetaSSL) {
                    throw new IllegalArgumentException(
                            "storage ssl is enabled, meta ssl must be enabled.");
                }

                if (sslSignType == null) {
                    throw new IllegalArgumentException("ssl is enabled, ssl sign type must not be "
                            + "null");
                }
                switch (sslSignType) {
                    case CA:
                        if (caSignParams == null) {
                            throw new IllegalArgumentException("ssl is enabled and sign type is "
                                    + "CA, caSignParam must not be null");
                        }
                        break;
                    case SELF:
                        if (selfSignParams == null) {
                            throw new IllegalArgumentException("ssl is enabled and sign type is "
                                    + "SELF, selfSignParam must not be null");
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
                    enableGraphSSL,
                    enableMetaSSL,
                    enableStorageSSL,
                    sslSignType,
                    caSignParams,
                    selfSignParams,
                    handshakeKey);
        }
    }
}
