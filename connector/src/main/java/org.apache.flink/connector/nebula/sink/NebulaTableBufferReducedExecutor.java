package org.apache.flink.connector.nebula.sink;

import com.vesoft.nebula.client.graph.net.Session;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.connector.sink.DynamicTableSink.DataStructureConverter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;

public class NebulaTableBufferReducedExecutor extends NebulaBatchExecutor<RowData> {
    private final DataStructureConverter dataStructureConverter;
    private final Function<Row, Row> keyExtractor;
    private final NebulaBatchExecutor<Row> insertExecutor;
    private final NebulaBatchExecutor<Row> deleteExecutor;
    private final Map<Row, Tuple2<Boolean, Row>> reduceBuffer = new HashMap<>();

    public NebulaTableBufferReducedExecutor(DataStructureConverter dataStructureConverter,
                                            Function<Row, Row> keyExtractor,
                                            NebulaBatchExecutor<Row> insertExecutor,
                                            NebulaBatchExecutor<Row> deleteExecutor) {
        this.dataStructureConverter = dataStructureConverter;
        this.keyExtractor = keyExtractor;
        this.insertExecutor = insertExecutor;
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
    public void clearBatch() {
        reduceBuffer.clear();
    }

    @Override
    public boolean isBatchEmpty() {
        return reduceBuffer.isEmpty();
    }

    @Override
    public void executeBatch(Session session) throws IOException {
        if (isBatchEmpty()) {
            return;
        }
        for (Tuple2<Boolean, Row> value : reduceBuffer.values()) {
            boolean isUpsert = value.f0;
            Row row = value.f1;
            if (isUpsert) {
                insertExecutor.addToBatch(row);
            } else {
                deleteExecutor.addToBatch(row);
            }
        }
        try {
            insertExecutor.executeBatch(session);
            deleteExecutor.executeBatch(session);
        } finally {
            insertExecutor.clearBatch();
            deleteExecutor.clearBatch();
        }
        clearBatch();
    }
}
