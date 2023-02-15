package org.apache.flink.connector.nebula.sink;

import java.util.Map;
import org.apache.flink.connector.nebula.connection.NebulaGraphConnectionProvider;
import org.apache.flink.connector.nebula.connection.NebulaMetaConnectionProvider;
import org.apache.flink.connector.nebula.statement.EdgeExecutionOptions;
import org.apache.flink.connector.nebula.utils.VidTypeEnum;
import org.apache.flink.types.Row;

public class NebulaEdgeBatchOutputFormat
        extends NebulaBatchOutputFormat<Row, EdgeExecutionOptions> {
    public NebulaEdgeBatchOutputFormat(NebulaGraphConnectionProvider graphProvider,
                                       NebulaMetaConnectionProvider metaProvider,
                                       EdgeExecutionOptions executionOptions) {
        super(graphProvider, metaProvider, executionOptions);
    }

    @Override
    protected NebulaBatchExecutor<Row> createNebulaBatchExecutor() {
        VidTypeEnum vidType = metaProvider.getVidType(metaClient, executionOptions.getGraphSpace());
        Map<String, Integer> schema = metaProvider.getEdgeSchema(
                metaClient,
                executionOptions.getGraphSpace(),
                executionOptions.getLabel());
        return new NebulaEdgeBatchExecutor(executionOptions, vidType, schema);
    }
}
