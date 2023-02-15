package org.apache.flink.connector.nebula.sink;

import java.util.Map;
import java.util.function.Function;
import org.apache.flink.connector.nebula.connection.NebulaGraphConnectionProvider;
import org.apache.flink.connector.nebula.connection.NebulaMetaConnectionProvider;
import org.apache.flink.connector.nebula.statement.EdgeExecutionOptions;
import org.apache.flink.connector.nebula.utils.VidTypeEnum;
import org.apache.flink.connector.nebula.utils.WriteModeEnum;
import org.apache.flink.table.connector.sink.DynamicTableSink.DataStructureConverter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;

public class NebulaEdgeBatchTableOutputFormat
        extends NebulaBatchOutputFormat<RowData, EdgeExecutionOptions> {
    private final DataStructureConverter dataStructureConverter;

    public NebulaEdgeBatchTableOutputFormat(NebulaGraphConnectionProvider graphProvider,
                                            NebulaMetaConnectionProvider metaProvider,
                                            EdgeExecutionOptions executionOptions,
                                            DataStructureConverter dataStructureConverter) {
        super(graphProvider, metaProvider, executionOptions);
        this.dataStructureConverter = dataStructureConverter;
    }

    @Override
    protected NebulaBatchExecutor<RowData> createNebulaBatchExecutor() {
        VidTypeEnum vidType = metaProvider.getVidType(metaClient, executionOptions.getGraphSpace());
        Map<String, Integer> schema = metaProvider.getEdgeSchema(
                metaClient,
                executionOptions.getGraphSpace(),
                executionOptions.getLabel());
        EdgeExecutionOptions insertOptions = executionOptions.toBuilder()
                .setWriteMode(WriteModeEnum.INSERT)
                .build();
        EdgeExecutionOptions deleteOptions = executionOptions.toBuilder()
                .setWriteMode(WriteModeEnum.DELETE)
                .build();
        return new NebulaTableBufferReducedExecutor(dataStructureConverter,
                createKeyExtractor(executionOptions.getSrcIndex(),
                        executionOptions.getDstIndex(),
                        executionOptions.getRankIndex()),
                new NebulaEdgeBatchExecutor(insertOptions, vidType, schema),
                new NebulaEdgeBatchExecutor(deleteOptions, vidType, schema));
    }

    private static Function<Row, Row> createKeyExtractor(int srcIdIndex,
                                                         int dstIdIndex, int rankIdIndex) {
        return row -> {
            Row key = new Row(3);
            key.setField(0, row.getField(srcIdIndex));
            key.setField(1, row.getField(dstIdIndex));
            if (rankIdIndex >= 0) {
                key.setField(2, row.getField(rankIdIndex));
            }
            return key;
        };
    }
}
