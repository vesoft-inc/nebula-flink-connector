package org.apache.flink.connector.nebula.sink;

import java.util.Map;
import java.util.function.Function;
import org.apache.flink.connector.nebula.connection.NebulaGraphConnectionProvider;
import org.apache.flink.connector.nebula.connection.NebulaMetaConnectionProvider;
import org.apache.flink.connector.nebula.statement.VertexExecutionOptions;
import org.apache.flink.connector.nebula.utils.VidTypeEnum;
import org.apache.flink.connector.nebula.utils.WriteModeEnum;
import org.apache.flink.table.connector.sink.DynamicTableSink.DataStructureConverter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;

public class NebulaVertexBatchTableOutputFormat
        extends NebulaBatchOutputFormat<RowData, VertexExecutionOptions> {
    private final DataStructureConverter dataStructureConverter;

    public NebulaVertexBatchTableOutputFormat(NebulaGraphConnectionProvider graphProvider,
                                              NebulaMetaConnectionProvider metaProvider,
                                              VertexExecutionOptions executionOptions,
                                              DataStructureConverter dataStructureConverter) {
        super(graphProvider, metaProvider, executionOptions);
        this.dataStructureConverter = dataStructureConverter;
    }

    @Override
    protected NebulaBatchExecutor<RowData> createNebulaBatchExecutor() {
        VidTypeEnum vidType = metaProvider.getVidType(metaClient, executionOptions.getGraphSpace());
        Map<String, Integer> schema = metaProvider.getTagSchema(
                metaClient,
                executionOptions.getGraphSpace(),
                executionOptions.getLabel());
        VertexExecutionOptions insertOptions = executionOptions.toBuilder()
                .setWriteMode(WriteModeEnum.INSERT)
                .build();
        VertexExecutionOptions deleteOptions = executionOptions.toBuilder()
                .setWriteMode(WriteModeEnum.DELETE)
                .build();
        return new NebulaTableBufferReducedExecutor(dataStructureConverter,
                createKeyExtractor(executionOptions.getIdIndex()),
                new NebulaVertexBatchExecutor(insertOptions, vidType, schema),
                new NebulaVertexBatchExecutor(deleteOptions, vidType, schema));
    }

    private static Function<Row, Row> createKeyExtractor(int idIndex) {
        return row -> {
            Row key = new Row(1);
            key.setField(0, row.getField(idIndex));
            return key;
        };
    }
}
