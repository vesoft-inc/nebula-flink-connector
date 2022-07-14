/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.source;

import com.vesoft.nebula.client.storage.StorageClient;
import com.vesoft.nebula.client.storage.data.BaseTableRow;
import com.vesoft.nebula.client.storage.data.VertexTableRow;
import com.vesoft.nebula.client.storage.scan.ScanVertexResult;
import com.vesoft.nebula.client.storage.scan.ScanVertexResultIterator;
import java.util.Iterator;
import java.util.List;
import org.apache.flink.connector.nebula.statement.ExecutionOptions;

/**
 * Nebula Graph Vertex reader
 */
public class NebulaVertexSource extends NebulaSource<BaseTableRow> {
    ScanVertexResultIterator iterator = null;
    Iterator<VertexTableRow> dataIterator = null;
    Iterator<Integer> scanPartIterator;

    public NebulaVertexSource(StorageClient storageClient, ExecutionOptions executionOptions,
                              List<Integer> scanParts) {
        super(storageClient, executionOptions);
        this.scanPartIterator = scanParts.iterator();
    }

    private void getVertexDataRow(int part) {
        if (executionOptions.isNoColumn()) {
            iterator = storageClient.scanVertex(
                    executionOptions.getGraphSpace(),
                    part,
                    executionOptions.getLabel(),
                    executionOptions.getLimit(),
                    executionOptions.getStartTime(),
                    executionOptions.getEndTime(),
                    true,
                    true);
        } else {
            iterator = storageClient.scanVertex(
                    executionOptions.getGraphSpace(),
                    part,
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
        if (dataIterator == null && iterator == null && !scanPartIterator.hasNext()) {
            return false;
        }

        while (dataIterator == null || !dataIterator.hasNext()) {
            if (iterator == null || !iterator.hasNext()) {
                if (scanPartIterator.hasNext()) {
                    getVertexDataRow(scanPartIterator.next());
                    continue;
                }
                break;
            } else {
                ScanVertexResult next = iterator.next();
                if (!next.isEmpty()) {
                    dataIterator = next.getVertexTableRows().iterator();
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
