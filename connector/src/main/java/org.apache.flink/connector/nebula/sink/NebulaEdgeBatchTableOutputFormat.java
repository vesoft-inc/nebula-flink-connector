/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.sink;

import java.util.List;
import java.util.function.Function;
import org.apache.flink.connector.nebula.connection.GraphProvider;
import org.apache.flink.connector.nebula.options.ConnectionOptions;
import org.apache.flink.connector.nebula.options.ExecutionOptions;
import org.apache.flink.connector.nebula.options.SinkEdgeOptions;
import org.apache.flink.connector.nebula.utils.NebulaEdgeSchema;
import org.apache.flink.connector.nebula.utils.WriteModeEnum;
import org.apache.flink.table.connector.sink.DynamicTableSink.DataStructureConverter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;

public class NebulaEdgeBatchTableOutputFormat
        extends NebulaBatchOutputFormat<RowData, SinkEdgeOptions> {
    private final DataStructureConverter dataStructureConverter;

    public NebulaEdgeBatchTableOutputFormat(SinkEdgeOptions edgeOptions,
                                            ConnectionOptions connectionOptions,
                                            DataStructureConverter dataStructureConverter) {
        super(connectionOptions, edgeOptions);
        this.dataStructureConverter = dataStructureConverter;
    }

    @Override
    protected NebulaBatchExecutor<RowData> createNebulaBatchExecutor() {
        try {
            NebulaEdgeSchema schema = graphProvider.getEdgeSchema(executionOptions.getGraphName(),
                                                                  executionOptions.getEdgeType());

            List<String> flinkSrcPkFields = executionOptions.getFlinkSrcPkFields();
            List<String> flinkDstPkFields = executionOptions.getFlinkDstPkFields();
            Function<Row, Row> keyExtractor = createKeyExtractor(flinkSrcPkFields,
                                                                 flinkDstPkFields);
            SinkEdgeOptions insertOptions = executionOptions.toBuilder()
                    .withWriteMode(WriteModeEnum.INSERTREPLACE)
                    .build();
            SinkEdgeOptions deleteOptions = executionOptions.toBuilder()
                    .withWriteMode(WriteModeEnum.DELETE)
                    .build();
            return new NebulaTableBufferReducedExecutor(dataStructureConverter,
                                                        keyExtractor,
                                                        new NebulaEdgeBatchExecutor(insertOptions,
                                                                                    schema),
                                                        new NebulaEdgeBatchExecutor(deleteOptions,
                                                                                    schema));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Function<Row, Row> createKeyExtractor(List<String> flinkSrcPkFields,
                                                         List<String> flinkDstPkFields) {
        return row -> {
            Row key   = new Row(flinkSrcPkFields.size() + flinkDstPkFields.size());
            int index = 0;
            for (String field : flinkSrcPkFields) {
                key.setField(index++, row.getField(field));
            }
            for (String field : flinkDstPkFields) {
                key.setField(index++, row.getField(field));
            }
            return key;
        };
    }
}
