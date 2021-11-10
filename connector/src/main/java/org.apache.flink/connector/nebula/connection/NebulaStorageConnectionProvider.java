/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.connection;


import com.vesoft.nebula.client.graph.data.HostAddress;
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
        StorageClient storageClient = new StorageClient(addresses);
        if (!storageClient.connect()) {
            throw new Exception("failed to connect storaged.");
        }
        return storageClient;
    }

}
