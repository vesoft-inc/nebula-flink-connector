/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.table;

import java.io.IOException;
import org.apache.flink.connector.nebula.connection.GraphProvider;
import org.apache.flink.connector.nebula.options.ConnectionOptions;
import org.apache.flink.connector.nebula.options.SourceExecutionOptions;
import org.apache.flink.connector.nebula.source.NebulaInputFormat;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

/**
 * implementation of NebulaInputFormat.
 * Read NebulaGraph data in nebula's {@link com.vesoft.nebula.driver.graph.scan.TableRow} format.
 */
public class NebulaRowDataInputFormat extends NebulaInputFormat<RowData> {

    private final LogicalType[] logicalTypes;

    public NebulaRowDataInputFormat(ConnectionOptions connectionOptions,
                                    SourceExecutionOptions executionOptions,
                                    LogicalType[] logicalTypes) {
        super(connectionOptions, executionOptions);
        this.logicalTypes = logicalTypes;
    }

    @Override
    public void open(InputSplit inputSplit) throws IOException {
        super.open(inputSplit);
        RowType rowType = RowType.of(logicalTypes);
        super.nebulaConverter = new NebulaRowDataConverter(rowType);
    }
}
