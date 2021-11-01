/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package org.apache.flink.connector.nebula.connection;


import com.vesoft.nebula.client.graph.NebulaPoolConfig;
import com.vesoft.nebula.client.graph.data.HostAddress;
import com.vesoft.nebula.client.graph.exception.AuthFailedException;
import com.vesoft.nebula.client.graph.exception.ClientServerIncompatibleException;
import com.vesoft.nebula.client.graph.exception.IOErrorException;
import com.vesoft.nebula.client.graph.exception.NotValidConnectionException;
import com.vesoft.nebula.client.graph.net.NebulaPool;
import com.vesoft.nebula.client.graph.net.Session;
import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.flink.connector.nebula.utils.NebulaConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NebulaGraphConnectionProvider implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(NebulaGraphConnectionProvider.class);

    private static final long serialVersionUID = 8392002706492085208L;

    private final NebulaClientOptions nebulaClientOptions;

    public NebulaGraphConnectionProvider(NebulaClientOptions nebulaClientOptions) {
        this.nebulaClientOptions = nebulaClientOptions;
    }

    /**
     * get Session to execute query statement
     */
    public NebulaPool getNebulaPool() throws UnknownHostException {
        List<HostAddress> addresses = new ArrayList<>();
        for (String address : nebulaClientOptions.getGraphAddress().split(NebulaConstant.COMMA)) {
            String[] hostAndPort = address.split(NebulaConstant.COLON);
            addresses.add(new HostAddress(hostAndPort[0], Integer.parseInt(hostAndPort[1])));
        }

        Collections.shuffle(addresses);
        NebulaPool nebulaPool = new NebulaPool();
        NebulaPoolConfig poolConfig = new NebulaPoolConfig();
        poolConfig.setTimeout(nebulaClientOptions.getTimeout());
        if (nebulaClientOptions.isEnableGraphSSL()) {
            poolConfig.setEnableSsl(true);
            switch (nebulaClientOptions.getSSLSighType()) {
                case CA:
                    poolConfig.setSslParam(nebulaClientOptions.getCaSignParam());
                    break;
                case SELF:
                    poolConfig.setSslParam(nebulaClientOptions.getSelfSignParam());
                    break;
                default:
                    throw new IllegalArgumentException("ssl sign type is not supported.");
            }
        }
        nebulaPool.init(addresses, poolConfig);
        return nebulaPool;
    }

    /**
     * get username
     */
    public String getUserName() {
        return nebulaClientOptions.getUsername();
    }

    /**
     * get password
     */
    public String getPassword() {
        return nebulaClientOptions.getPassword();
    }
}
