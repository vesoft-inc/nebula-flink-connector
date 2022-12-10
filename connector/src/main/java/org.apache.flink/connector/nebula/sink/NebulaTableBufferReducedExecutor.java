package org.apache.flink.connector.nebula.sink;

import com.vesoft.nebula.client.graph.net.Session;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.connector.sink.DynamicTableSink.DataStructureConverter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;

public class NebulaTableBufferReducedExecutor implements NebulaBatchExecutor<RowData> {
    private final DataStructureConverter dataStructureConverter;
    private final Function<Row, Row> keyExtractor;
    private final NebulaBatchExecutor<Row> upsertExecutor;
    private final NebulaBatchExecutor<Row> deleteExecutor;
    private final Map<Row, Tuple2<Boolean, Row>> reduceBuffer = new HashMap<>();

    public NebulaTableBufferReducedExecutor(DataStructureConverter dataStructureConverter,
                                            Function<Row, Row> keyExtractor,
                                            NebulaBatchExecutor<Row> upsertExecutor,
                                            NebulaBatchExecutor<Row> deleteExecutor) {
        this.dataStructureConverter = dataStructureConverter;
        this.keyExtractor = keyExtractor;
        this.upsertExecutor = upsertExecutor;
        this.deleteExecutor = deleteExecutor;
    }

    @Override
    public void addToBatch(RowData record) {
        boolean isUpsert;
        switch (record.getRowKind()) {
            case INSERT:
            case UPDATE_AFTER:
                isUpsert = true;
                break;
            case UPDATE_BEFORE:
            case DELETE:
                isUpsert = false;
                break;
            default:
                return;
        }
        Row row = (Row) dataStructureConverter.toExternal(record);
        Row key = keyExtractor.apply(row);
        reduceBuffer.put(key, Tuple2.of(isUpsert, row));
    }

    @Override
    public String executeBatch(Session session) {
        for (Tuple2<Boolean, Row> value : reduceBuffer.values()) {
            boolean isUpsert = value.f0;
            Row row = value.f1;
            if (isUpsert) {
                upsertExecutor.addToBatch(row);
            } else {
                deleteExecutor.addToBatch(row);
            }
        }
        String upsertErrorStatement = upsertExecutor.executeBatch(session);
        String deleteErrorStatement = deleteExecutor.executeBatch(session);
        reduceBuffer.clear();
        String errorStatements = Stream.of(upsertErrorStatement, deleteErrorStatement)
                .filter(Objects::nonNull).collect(Collectors.joining("; "));
        return errorStatements.isEmpty() ? null : errorStatements;
    }
}
