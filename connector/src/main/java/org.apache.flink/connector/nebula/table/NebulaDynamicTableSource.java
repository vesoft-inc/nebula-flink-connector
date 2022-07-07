/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.table;


import java.util.Arrays;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.connector.nebula.connection.NebulaClientOptions;
import org.apache.flink.connector.nebula.connection.NebulaStorageConnectionProvider;
import org.apache.flink.connector.nebula.statement.ExecutionOptions;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

public class NebulaDynamicTableSource implements ScanTableSource {

    private final NebulaClientOptions nebulaClientOptions;
    private final ExecutionOptions executionOptions;
    private final TableSchema tableSchema;

    public NebulaDynamicTableSource(NebulaClientOptions nebulaClientOptions,
                                    ExecutionOptions executionOptions,
                                    TableSchema tableSchema) {
        this.nebulaClientOptions = nebulaClientOptions;
        this.executionOptions = executionOptions;
        this.tableSchema = tableSchema;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        DataType[] fieldDataTypes = tableSchema.getFieldDataTypes();
        LogicalType[] logicalTypes = Arrays.stream(fieldDataTypes)
                .map(DataType::getLogicalType)
                .toArray(LogicalType[]::new);

        InputFormat<RowData, InputSplit> inputFormat = new NebulaRowDataInputFormat(
                new NebulaStorageConnectionProvider(this.nebulaClientOptions),
                this.executionOptions,
                logicalTypes
        );
        return InputFormatProvider.of(inputFormat);
    }

    @Override
    public DynamicTableSource copy() {
        return new NebulaDynamicTableSource(nebulaClientOptions, executionOptions, tableSchema);
    }

    @Override
    public String asSummaryString() {
        return "NebulaDynamicTableSource";
    }
}
