/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package org.apache.flink.connector.nebula.connection;

import com.vesoft.nebula.client.graph.data.HostAddress;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.connector.nebula.utils.NebulaConstant;

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
            if (metaAddress == null || metaAddress.trim().isEmpty()) {
                throw new IllegalArgumentException("meta address can not be empty.");
            }

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
