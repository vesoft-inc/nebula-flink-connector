/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.table;

import org.apache.flink.connector.nebula.connection.GraphProvider;
import org.apache.flink.connector.nebula.options.ConnectionOptions;
import org.apache.flink.connector.nebula.options.ExecutionOptions;
import org.apache.flink.connector.nebula.options.SinkEdgeOptions;
import org.apache.flink.connector.nebula.options.SinkNodeOptions;
import org.apache.flink.connector.nebula.sink.NebulaBatchOutputFormat;
import org.apache.flink.connector.nebula.sink.NebulaEdgeBatchTableOutputFormat;
import org.apache.flink.connector.nebula.sink.NebulaNodeBatchTableOutputFormat;
import org.apache.flink.connector.nebula.sink.NebulaSinkFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

public class NebulaDynamicTableSink implements DynamicTableSink {
    private final ConnectionOptions connectionOptions;
    private final ExecutionOptions  executionOptions;
    final         DataType          producedDataType;

    public NebulaDynamicTableSink(ConnectionOptions connectionOptions,
                                  ExecutionOptions executionOptions, DataType producedDataType) {
        this.connectionOptions = connectionOptions;
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
        GraphProvider graphProvider = new GraphProvider(connectionOptions);
        DataStructureConverter converter =
                context.createDataStructureConverter(producedDataType);
        NebulaBatchOutputFormat<RowData, ?> outputFormat;
        if (executionOptions instanceof SinkNodeOptions) {
            outputFormat = new NebulaNodeBatchTableOutputFormat((SinkNodeOptions) executionOptions,
                                                                connectionOptions,
                                                                converter);
        } else if (executionOptions instanceof SinkEdgeOptions) {
            outputFormat = new NebulaEdgeBatchTableOutputFormat(
                    (SinkEdgeOptions) executionOptions, connectionOptions, converter);
        } else {
            throw new IllegalArgumentException("unknown execution options type");
        }
        NebulaSinkFunction<RowData> sinkFunction = new NebulaSinkFunction<>(outputFormat);
        return SinkFunctionProvider.of(sinkFunction);
    }

    @Override
    public DynamicTableSink copy() {
        return new NebulaDynamicTableSink(connectionOptions, executionOptions, producedDataType);
    }

    @Override
    public String asSummaryString() {
        return "NebulaDynamicTableSink";
    }
}
