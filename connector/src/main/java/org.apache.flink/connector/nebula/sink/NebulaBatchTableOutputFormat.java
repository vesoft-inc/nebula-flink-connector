package org.apache.flink.connector.nebula.sink;

import java.util.Map;
import org.apache.flink.connector.nebula.connection.NebulaGraphConnectionProvider;
import org.apache.flink.connector.nebula.connection.NebulaMetaConnectionProvider;
import org.apache.flink.connector.nebula.utils.VidTypeEnum;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;

public class NebulaBatchTableOutputFormat extends NebulaBatchOutputFormat<RowData> {
    private final DynamicTableSink.DataStructureConverter converter;

    public NebulaBatchTableOutputFormat(
            NebulaGraphConnectionProvider graphProvider,
            NebulaMetaConnectionProvider metaProvider,
            DynamicTableSink.DataStructureConverter converter) {
        super(graphProvider, metaProvider);
        this.converter = converter;
    }

    protected void setNebulaBatchExecutor() {
        VidTypeEnum vidType = metaProvider.getVidType(metaClient, executionOptions.getGraphSpace());
        boolean isVertex = executionOptions.getDataType().isVertex();
        Map<String, Integer> schema;
        if (isVertex) {
            schema =
                    metaProvider.getTagSchema(
                            metaClient,
                            executionOptions.getGraphSpace(),
                            executionOptions.getLabel());
            nebulaBatchExecutor =
                    new NebulaVertexTableBatchExecutor(
                            executionOptions, vidType, schema, converter);
        } else {
            schema =
                    metaProvider.getEdgeSchema(
                            metaClient,
                            executionOptions.getGraphSpace(),
                            executionOptions.getLabel());
            nebulaBatchExecutor =
                    new NebulaEdgeTableBatchExecutor(executionOptions, vidType, schema, converter);
        }
    }
}
