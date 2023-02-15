package org.apache.flink.connector.nebula.sink;

import java.util.Map;
import org.apache.flink.connector.nebula.connection.NebulaGraphConnectionProvider;
import org.apache.flink.connector.nebula.connection.NebulaMetaConnectionProvider;
import org.apache.flink.connector.nebula.statement.VertexExecutionOptions;
import org.apache.flink.connector.nebula.utils.VidTypeEnum;
import org.apache.flink.types.Row;

public class NebulaVertexBatchOutputFormat
        extends NebulaBatchOutputFormat<Row, VertexExecutionOptions> {

    public NebulaVertexBatchOutputFormat(NebulaGraphConnectionProvider graphProvider,
                                         NebulaMetaConnectionProvider metaProvider,
                                         VertexExecutionOptions executionOptions) {
        super(graphProvider, metaProvider, executionOptions);
    }

    @Override
    protected NebulaBatchExecutor<Row> createNebulaBatchExecutor() {
        VidTypeEnum vidType = metaProvider.getVidType(metaClient, executionOptions.getGraphSpace());
        Map<String, Integer> schema = metaProvider.getTagSchema(
                metaClient,
                executionOptions.getGraphSpace(),
                executionOptions.getLabel());
        return new NebulaVertexBatchExecutor(executionOptions, vidType, schema);
    }
}
