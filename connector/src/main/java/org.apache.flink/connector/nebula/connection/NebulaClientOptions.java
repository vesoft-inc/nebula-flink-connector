/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package org.apache.flink.connector.nebula.connection;

import java.io.Serializable;

public class NebulaClientOptions implements Serializable {

    private static final long serialVersionUID = 5685521189643221375L;

    private final String metaAddress;

    private final String graphAddress;

    private final String username;

    private final String password;

    private final int timeout;

    private final int connectRetry;

    private NebulaClientOptions(String metaAddress, String graphAddress, String username,
                                String password, int timeout, int connectRetry) {
        this.metaAddress = metaAddress;
        this.graphAddress = graphAddress;
        this.username = username;
        this.password = password;
        this.timeout = timeout;
        this.connectRetry = connectRetry;
    }

    public String getMetaAddress() {
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

        public NebulaClientOptions build() {
            return new NebulaClientOptions(
                    metaAddress,
                    graphAddress,
                    username,
                    password,
                    timeout,
                    connectRetry);
        }
    }
}
