/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package org.apache.flink.connector.nebula.source;

import com.vesoft.nebula.client.storage.StorageClient;
import com.vesoft.nebula.client.storage.data.BaseTableRow;
import com.vesoft.nebula.client.storage.data.EdgeTableRow;
import com.vesoft.nebula.client.storage.scan.ScanEdgeResult;
import com.vesoft.nebula.client.storage.scan.ScanEdgeResultIterator;
import com.vesoft.nebula.client.storage.scan.ScanVertexResult;
import java.util.Iterator;
import org.apache.flink.connector.nebula.statement.ExecutionOptions;

/**
 * Nebula Graph Edge reader
 */
public class NebulaEdgeSource extends NebulaSource {
    ScanEdgeResultIterator iterator = null;
    Iterator<EdgeTableRow> dataIterator = null;


    public NebulaEdgeSource(StorageClient storageClient, ExecutionOptions executionOptions) {
        super(storageClient, executionOptions);
    }

    public void getEdgeDataRow() {
        if (executionOptions.isNoColumn()) {
            iterator = storageClient.scanEdge(
                    executionOptions.getGraphSpace(),
                    executionOptions.getLabel(),
                    executionOptions.getLimit(),
                    executionOptions.getStartTime(),
                    executionOptions.getEndTime(),
                    true,
                    true
            );
        } else {
            iterator = storageClient.scanEdge(
                    executionOptions.getGraphSpace(),
                    executionOptions.getLabel(),
                    executionOptions.getFields(),
                    executionOptions.getLimit(),
                    executionOptions.getStartTime(),
                    executionOptions.getEndTime(),
                    true,
                    true
            );
        }
    }

    @Override
    public boolean hasNext() throws Exception {
        if (iterator == null) {
            getEdgeDataRow();
        }
        if (dataIterator == null || !dataIterator.hasNext()) {
            if (!iterator.hasNext()) {
                return false;
            }
            while (iterator.hasNext()) {
                ScanEdgeResult result = iterator.next();
                if (!result.isEmpty()) {
                    dataIterator = result.getEdgeTableRows().iterator();
                    break;
                }
            }
        }
        if (dataIterator == null) {
            return false;
        }
        return dataIterator.hasNext();
    }

    @Override
    public BaseTableRow next() {
        return dataIterator.next();
    }
}
