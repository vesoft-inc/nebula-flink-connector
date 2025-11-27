/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.source;

import com.vesoft.nebula.driver.graph.scan.ScanEdgeResult;
import com.vesoft.nebula.driver.graph.scan.ScanEdgeResultIterator;
import com.vesoft.nebula.driver.graph.scan.TableRow;
import java.util.Iterator;
import java.util.List;
import org.apache.flink.connector.nebula.options.ConnectionOptions;
import org.apache.flink.connector.nebula.options.SourceExecutionOptions;

/**
 * Nebula Graph Edge reader
 */
public class NebulaEdgeSource extends NebulaSource {
    ScanEdgeResultIterator iterator     = null;
    Iterator<TableRow>     dataIterator = null;
    Iterator<Integer>      scanPartIterator;

    public NebulaEdgeSource(ConnectionOptions connectionOptions,
                            SourceExecutionOptions executionOptions,
                            List<Integer> scanParts) {
        super(connectionOptions, executionOptions);
        this.scanPartIterator = scanParts.iterator();
    }

    public void getEdgeDataRow(int part) {
        iterator = graphProvider.scanEdge(executionOptions.getSchema(),
                                          executionOptions.getGraphName(),
                                          executionOptions.getTypeName(),
                                          executionOptions.getReturnCols(),
                                          part,
                                          executionOptions.getBatchSize());

    }

    @Override
    public boolean hasNext() throws Exception {
        if (dataIterator == null && iterator == null && !scanPartIterator.hasNext()) {
            return false;
        }

        while (dataIterator == null || !dataIterator.hasNext()) {
            if (iterator == null || !iterator.hasNext()) {
                if (scanPartIterator.hasNext()) {
                    getEdgeDataRow(scanPartIterator.next());
                    continue;
                }
                break;
            } else {
                ScanEdgeResult next = iterator.next();
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
