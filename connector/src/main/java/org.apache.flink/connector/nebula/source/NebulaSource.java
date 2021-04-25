/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package org.apache.flink.connector.nebula.source;

import com.vesoft.nebula.client.storage.StorageClient;
import com.vesoft.nebula.client.storage.data.BaseTableRow;
import org.apache.flink.connector.nebula.statement.ExecutionOptions;

/**
 * NebulaSource is the reader to read NebulaGraph's data iteratively.
 */
abstract class NebulaSource {

    StorageClient storageClient;
    ExecutionOptions executionOptions;

    public NebulaSource(StorageClient storageClient, ExecutionOptions executionOptions) {
        this.storageClient = storageClient;
        this.executionOptions = executionOptions;
    }

    /**
     * if source has more data
     */
    abstract boolean hasNext() throws Exception;

    /**
     * get another Nebula Graph data
     */
    abstract BaseTableRow next();
}
