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
import org.apache.flink.connector.nebula.options.SinkNodeOptions;
import org.apache.flink.connector.nebula.utils.NebulaNodeSchema;
import org.apache.flink.connector.nebula.utils.WriteModeEnum;
import org.apache.flink.table.connector.sink.DynamicTableSink.DataStructureConverter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;

public class NebulaNodeBatchTableOutputFormat
        extends NebulaBatchOutputFormat<RowData, SinkNodeOptions> {
    private final DataStructureConverter dataStructureConverter;

    public NebulaNodeBatchTableOutputFormat(SinkNodeOptions sinkNodeOptions,
                                            ConnectionOptions connectionOptions,
                                            DataStructureConverter dataStructureConverter) {
        super(connectionOptions, sinkNodeOptions);
        this.dataStructureConverter = dataStructureConverter;
    }

    @Override
    protected NebulaBatchExecutor<RowData> createNebulaBatchExecutor() {
        try {
            NebulaNodeSchema schema = graphProvider.getNodeSchema(
                    executionOptions.getGraphName(),
                    executionOptions.getNodeType());
            SinkNodeOptions insertOptions = executionOptions.toBuilder()
                    .withWriteMode(WriteModeEnum.INSERTREPLACE)
                    .build();
            SinkNodeOptions deleteOptions = executionOptions.toBuilder()
                    .withWriteMode(WriteModeEnum.DELETE)
                    .build();
            Function<Row, Row> keyExtractor = createKeyExtractor(executionOptions.getFlinkFields(),
                                                                 executionOptions.getNebulaFields(),
                                                                 schema);
            return new NebulaTableBufferReducedExecutor(dataStructureConverter,
                                                        keyExtractor,
                                                        new NebulaNodeBatchExecutor(insertOptions,
                                                                                    schema),
                                                        new NebulaNodeBatchExecutor(deleteOptions,
                                                                                    schema));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Function<Row, Row> createKeyExtractor(List<String> flinkFields,
                                                         List<String> nebulaFields,
                                                         NebulaNodeSchema schema) {
        return row -> {
            Row key = new Row(schema.getPkNames().size());
            for (int i = 0; i < schema.getPropNames().size(); i++) {
                int index = nebulaFields.indexOf(schema.getPkNames().get(i));
                key.setField(i, row.getField(flinkFields.get(index)));
            }
            return key;
        };
    }
}
