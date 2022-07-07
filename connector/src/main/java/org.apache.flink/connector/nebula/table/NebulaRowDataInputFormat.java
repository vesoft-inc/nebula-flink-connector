/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.table;

import java.io.IOException;

import com.vesoft.nebula.client.storage.data.BaseTableRow;
import org.apache.flink.connector.nebula.connection.NebulaStorageConnectionProvider;
import org.apache.flink.connector.nebula.source.NebulaInputFormat;
import org.apache.flink.connector.nebula.statement.ExecutionOptions;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

/**
 * implementation of NebulaInputFormat.
 * Read NebulaGraph data in nebula's {@link BaseTableRow} format.
 * <b>how to use:
 *   NebulaInputTableRowFormat inputFormat = new NebulaInputTableRowFormat
 *                                          (storageConnectionProvider, vertexExecutionOptions);
 *   DataSource dataSource = env.createInput(inputFormat);
 * </b>
 */
public class NebulaRowDataInputFormat extends NebulaInputFormat<RowData> {

    private final LogicalType[] logicalTypes;

    public NebulaRowDataInputFormat(NebulaStorageConnectionProvider storageConnectionProvider,
                                    ExecutionOptions executionOptions,
                                    LogicalType[] logicalTypes) {
        super(storageConnectionProvider, executionOptions);
        this.logicalTypes = logicalTypes;
    }

    @Override
    public void open(InputSplit inputSplit) throws IOException {
        super.open(inputSplit);
        RowType rowType = RowType.of(logicalTypes);
        super.nebulaConverter = new NebulaRowDataConverter(rowType);
    }
}
