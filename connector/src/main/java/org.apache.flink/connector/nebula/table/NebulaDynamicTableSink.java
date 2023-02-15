package org.apache.flink.connector.nebula.table;

import org.apache.flink.connector.nebula.connection.NebulaClientOptions;
import org.apache.flink.connector.nebula.connection.NebulaGraphConnectionProvider;
import org.apache.flink.connector.nebula.connection.NebulaMetaConnectionProvider;
import org.apache.flink.connector.nebula.sink.NebulaBatchOutputFormat;
import org.apache.flink.connector.nebula.sink.NebulaEdgeBatchTableOutputFormat;
import org.apache.flink.connector.nebula.sink.NebulaSinkFunction;
import org.apache.flink.connector.nebula.sink.NebulaVertexBatchTableOutputFormat;
import org.apache.flink.connector.nebula.statement.EdgeExecutionOptions;
import org.apache.flink.connector.nebula.statement.ExecutionOptions;
import org.apache.flink.connector.nebula.statement.VertexExecutionOptions;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

public class NebulaDynamicTableSink implements DynamicTableSink {
    private final String metaAddress;
    private final String graphAddress;

    private final String username;
    private final String password;
    private final ExecutionOptions executionOptions;
    final DataType producedDataType;

    public NebulaDynamicTableSink(NebulaClientOptions clientOptions,
                                  ExecutionOptions executionOptions, DataType producedDataType) {
        this.metaAddress = clientOptions.getRawMetaAddress();
        this.graphAddress = clientOptions.getGraphAddress();
        this.username = clientOptions.getUsername();
        this.password = clientOptions.getPassword();
        this.executionOptions = executionOptions;
        this.producedDataType = producedDataType;
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

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {

        NebulaClientOptions builder = new NebulaClientOptions.NebulaClientOptionsBuilder()
                .setMetaAddress(metaAddress)
                .setGraphAddress(graphAddress)
                .setUsername(username)
                .setPassword(password)
                .build();

        NebulaGraphConnectionProvider graphProvider = new NebulaGraphConnectionProvider(builder);
        NebulaMetaConnectionProvider metaProvider = new NebulaMetaConnectionProvider(builder);
        DataStructureConverter converter =
                context.createDataStructureConverter(producedDataType);
        NebulaBatchOutputFormat<RowData, ?> outputFormat;
        if (executionOptions instanceof VertexExecutionOptions) {
            outputFormat = new NebulaVertexBatchTableOutputFormat(graphProvider, metaProvider,
                    (VertexExecutionOptions) executionOptions, converter);
        } else if (executionOptions instanceof EdgeExecutionOptions) {
            outputFormat = new NebulaEdgeBatchTableOutputFormat(graphProvider, metaProvider,
                    (EdgeExecutionOptions) executionOptions, converter);
        } else {
            throw new IllegalArgumentException("unknown execution options type");
        }
        NebulaSinkFunction<RowData> sinkFunction = new NebulaSinkFunction<>(outputFormat);
        return SinkFunctionProvider.of(sinkFunction);
    }

    @Override
    public DynamicTableSink copy() {
        return null;
    }

    @Override
    public String asSummaryString() {
        return null;
    }
}
