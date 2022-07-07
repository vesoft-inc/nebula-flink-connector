package org.apache.flink.connector.nebula.table;

import org.apache.flink.connector.nebula.connection.NebulaClientOptions;
import org.apache.flink.connector.nebula.connection.NebulaGraphConnectionProvider;
import org.apache.flink.connector.nebula.connection.NebulaMetaConnectionProvider;
import org.apache.flink.connector.nebula.statement.ExecutionOptions;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.OutputFormatProvider;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;

import java.util.Arrays;

public class NebulaDynamicTableSink implements DynamicTableSink {
    private final NebulaClientOptions nebulaClientOptions;
    private final ExecutionOptions executionOptions;
    private final TableSchema tableSchema;

    public NebulaDynamicTableSink(NebulaClientOptions nebulaClientOptions,
                                  ExecutionOptions executionOptions,
                                  TableSchema tableSchema) {
        this.nebulaClientOptions = nebulaClientOptions;
        this.executionOptions = executionOptions;
        this.tableSchema = tableSchema;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        ChangelogMode.Builder builder = ChangelogMode.newBuilder();
        for (RowKind kind : requestedMode.getContainedKinds()) {
            if (kind != RowKind.UPDATE_BEFORE) {
                builder.addContainedKind(kind);
            }
        }
        return builder.build();
    }

    // @Override
    // public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
    //     DataStructureConverter converter =
    //             context.createDataStructureConverter(tableSchema.toPhysicalRowDataType());
    //     NebulaGraphConnectionProvider graphProvider =
    //             new NebulaGraphConnectionProvider(nebulaClientOptions);
    //     NebulaMetaConnectionProvider metaProvider =
    //             new NebulaMetaConnectionProvider(nebulaClientOptions);
    //     NebulaBatchOutputFormat<RowData> outPutFormat =
    //             new NebulaRowDataOutputFormat(graphProvider, metaProvider, converter);
    //     outPutFormat.setExecutionOptions(executionOptions);
    //     NebulaSinkFunction<RowData> sinkFunction = new NebulaSinkFunction<>(outPutFormat);
    //     return SinkFunctionProvider.of(sinkFunction);
    //     // return OutputFormatProvider.of(outPutFormat);
    // }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        DataType[] fieldDataTypes = tableSchema.getFieldDataTypes();
        LogicalType[] logicalTypes = Arrays.stream(fieldDataTypes)
                .map(DataType::getLogicalType)
                .toArray(LogicalType[]::new);
        NebulaGraphConnectionProvider graphProvider =
                new NebulaGraphConnectionProvider(nebulaClientOptions);
        NebulaMetaConnectionProvider metaProvider =
                new NebulaMetaConnectionProvider(nebulaClientOptions);
        NebulaRowDataOutputFormat outPutFormat =
                new NebulaRowDataOutputFormat(graphProvider, metaProvider, logicalTypes);
        outPutFormat.setExecutionOptions(executionOptions);
        return OutputFormatProvider.of(outPutFormat);
    }

    @Override
    public DynamicTableSink copy() {
        return new NebulaDynamicTableSink(nebulaClientOptions, executionOptions, tableSchema);
    }

    @Override
    public String asSummaryString() {
        return "NebulaDynamicTableSink";
    }
}
