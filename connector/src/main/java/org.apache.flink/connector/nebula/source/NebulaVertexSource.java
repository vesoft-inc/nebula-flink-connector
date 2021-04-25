/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package org.apache.flink.connector.nebula.source;

import com.vesoft.nebula.client.storage.StorageClient;
import com.vesoft.nebula.client.storage.data.BaseTableRow;
import com.vesoft.nebula.client.storage.data.VertexTableRow;
import com.vesoft.nebula.client.storage.scan.ScanVertexResult;
import com.vesoft.nebula.client.storage.scan.ScanVertexResultIterator;
import java.util.Iterator;
import org.apache.flink.connector.nebula.statement.ExecutionOptions;

/**
 * Nebula Graph Vertex reader
 */
public class NebulaVertexSource extends NebulaSource {
    ScanVertexResultIterator iterator = null;
    Iterator<VertexTableRow> dataIterator = null;


    public NebulaVertexSource(StorageClient storageClient, ExecutionOptions executionOptions) {
        super(storageClient, executionOptions);
    }

    private void getVertexDataRow() {
        if (executionOptions.isNoColumn()) {
            iterator = storageClient.scanVertex(
                    executionOptions.getGraphSpace(),
                    executionOptions.getLabel(),
                    executionOptions.getLimit(),
                    executionOptions.getStartTime(),
                    executionOptions.getEndTime(),
                    true,
                    true);
        } else {
            iterator = storageClient.scanVertex(
                    executionOptions.getGraphSpace(),
                    executionOptions.getLabel(),
                    executionOptions.getFields(),
                    executionOptions.getLimit(),
                    executionOptions.getStartTime(),
                    executionOptions.getEndTime(),
                    true,
                    true);
        }
    }

    @Override
    public boolean hasNext() throws Exception {
        if (iterator == null) {
            getVertexDataRow();
        }

        if (dataIterator == null || !dataIterator.hasNext()) {
            if (!iterator.hasNext()) {
                return false;
            }
            while (iterator.hasNext()) {
                ScanVertexResult result = iterator.next();
                if (!result.isEmpty()) {
                    dataIterator = result.getVertexTableRows().iterator();
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
