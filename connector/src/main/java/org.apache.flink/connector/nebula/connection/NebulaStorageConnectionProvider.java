/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.connection;

import com.vesoft.nebula.client.graph.data.CASignedSSLParam;
import com.vesoft.nebula.client.graph.data.HostAddress;
import com.vesoft.nebula.client.graph.data.SSLParam;
import com.vesoft.nebula.client.graph.data.SelfSignedSSLParam;
import com.vesoft.nebula.client.storage.StorageClient;
import java.io.Serializable;
import java.util.List;

public class NebulaStorageConnectionProvider implements Serializable {

    private static final long serialVersionUID = -3822165815516596188L;

    private NebulaClientOptions nebulaClientOptions;

    public NebulaStorageConnectionProvider(NebulaClientOptions nebulaClientOptions) {
        this.nebulaClientOptions = nebulaClientOptions;
    }

    public NebulaStorageConnectionProvider() {
    }

    public StorageClient getStorageClient() throws Exception {
        List<HostAddress> addresses = nebulaClientOptions.getMetaAddress();
        int timeout = nebulaClientOptions.getTimeout();
        int retry = nebulaClientOptions.getConnectRetry();
        StorageClient storageClient;
        if (nebulaClientOptions.isEnableStorageSSL()) {
            switch (nebulaClientOptions.getSSLSighType()) {
                case CA: {
                    CASignParams caSignParams = nebulaClientOptions.getCaSignParam();
                    SSLParam sslParam = new CASignedSSLParam(caSignParams.getCaCrtFilePath(),
                            caSignParams.getCrtFilePath(), caSignParams.getKeyFilePath());
                    storageClient = new StorageClient(addresses, timeout, retry, retry, true,
                            sslParam);
                    break;
                }
                case SELF: {
                    SelfSignParams selfSignParams = nebulaClientOptions.getSelfSignParam();
                    SSLParam sslParam = new SelfSignedSSLParam(selfSignParams.getCrtFilePath(),
                            selfSignParams.getKeyFilePath(), selfSignParams.getPassword());
                    storageClient = new StorageClient(addresses, timeout, retry, retry, true,
                            sslParam);
                    break;
                }
                default:
                    throw new IllegalArgumentException("ssl sign type is not supported.");
            }
        } else {
            storageClient = new StorageClient(addresses, timeout);
        }

        if (!storageClient.connect()) {
            throw new Exception("failed to connect storaged.");
        }
        return storageClient;
    }

    public NebulaClientOptions getNebulaClientOptions() {
        return nebulaClientOptions;
    }

    public void setNebulaClientOptions(NebulaClientOptions nebulaClientOptions) {
        this.nebulaClientOptions = nebulaClientOptions;
    }
}
