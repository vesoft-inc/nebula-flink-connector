/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.source;

import com.vesoft.nebula.driver.graph.scan.ScanNodeResult;
import com.vesoft.nebula.driver.graph.scan.ScanNodeResultIterator;
import com.vesoft.nebula.driver.graph.scan.TableRow;
import java.util.Iterator;
import java.util.List;
import org.apache.flink.connector.nebula.options.ConnectionOptions;
import org.apache.flink.connector.nebula.options.SourceExecutionOptions;

/**
 * Nebula Graph Vertex reader
 */
public class NebulaNodeSource extends NebulaSource {
    ScanNodeResultIterator iterator     = null;
    Iterator<TableRow>     dataIterator = null;
    Iterator<Integer>      scanPartIterator;

    public NebulaNodeSource(ConnectionOptions connectionOptions,
                            SourceExecutionOptions executionOptions,
                            List<Integer> scanParts) {
        super(connectionOptions, executionOptions);
        this.scanPartIterator = scanParts.iterator();
    }

    private void getNodeDataRow(int part) {
        iterator = graphProvider.scanNode(executionOptions.getSchema(),
                                          executionOptions.getGraphName(),
                                          executionOptions.getTypeName(),
                                          executionOptions.getReturnCols(),
                                          part,
                                          executionOptions.getBatchSize());
    }

    @Override
    public boolean hasNext() {
        if (dataIterator == null && iterator == null && !scanPartIterator.hasNext()) {
            return false;
        }

        while (dataIterator == null || !dataIterator.hasNext()) {
            if (iterator == null || !iterator.hasNext()) {
                if (scanPartIterator.hasNext()) {
                    getNodeDataRow(scanPartIterator.next());
                    continue;
                }
                break;
            } else {
                ScanNodeResult next = iterator.next();
                if (!next.isEmpty()) {
                    dataIterator = next.getTableRows().iterator();
                }
            }
        }

        if (dataIterator == null) {
            return false;
        }
        return dataIterator.hasNext();
    }

    @Override
    public TableRow next() {
        return dataIterator.next();
    }
}
