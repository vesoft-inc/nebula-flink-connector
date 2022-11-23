package org.apache.flink.connector.nebula.sink;

import com.vesoft.nebula.client.graph.net.Session;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.nebula.utils.WriteModeEnum;
import org.apache.flink.table.connector.sink.DynamicTableSink.DataStructureConverter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;

public class NebulaTableBufferReducedExecutor implements NebulaBatchExecutor<RowData> {
    private final DataStructureConverter dataStructureConverter;
    private final Function<Row, Row> keyExtractor;
    private final NebulaBatchExecutor<Row> insertExecutor;
    private final NebulaBatchExecutor<Row> updateExecutor;
    private final NebulaBatchExecutor<Row> deleteExecutor;
    private final Map<Row, Tuple2<WriteModeEnum, Row>> reduceBuffer = new HashMap<>();

    public NebulaTableBufferReducedExecutor(DataStructureConverter dataStructureConverter,
                                            Function<Row, Row> keyExtractor,
                                            NebulaBatchExecutor<Row> insertExecutor,
                                            NebulaBatchExecutor<Row> updateExecutor,
                                            NebulaBatchExecutor<Row> deleteExecutor) {
        this.dataStructureConverter = dataStructureConverter;
        this.keyExtractor = keyExtractor;
        this.insertExecutor = insertExecutor;
        this.updateExecutor = updateExecutor;
        this.deleteExecutor = deleteExecutor;
    }

    @Override
    public void addToBatch(RowData record) {
        WriteModeEnum writeMode;
        switch (record.getRowKind()) {
            case INSERT:
                writeMode = WriteModeEnum.INSERT;
                break;
            case UPDATE_AFTER:
                writeMode = WriteModeEnum.UPDATE;
                break;
            case DELETE:
                writeMode = WriteModeEnum.DELETE;
                break;
            default:
                return;
        }
        Row row = (Row) dataStructureConverter.toExternal(record);
        Row key = keyExtractor.apply(row);
        reduceBuffer.put(key, Tuple2.of(writeMode, row));
    }

    @Override
    public String executeBatch(Session session) {
        for (Tuple2<WriteModeEnum, Row> value : reduceBuffer.values()) {
            WriteModeEnum writeMode = value.f0;
            Row row = value.f1;
            switch (writeMode) {
                case INSERT:
                    insertExecutor.addToBatch(row);
                    break;
                case UPDATE:
                    updateExecutor.addToBatch(row);
                    break;
                case DELETE:
                    deleteExecutor.addToBatch(row);
                    break;
                default:
            }
        }
        String insertErrorStatement = insertExecutor.executeBatch(session);
        String updateErrorStatement = updateExecutor.executeBatch(session);
        String deleteErrorStatement = deleteExecutor.executeBatch(session);
        reduceBuffer.clear();
        String errorStatements = Stream.of(
                insertErrorStatement, updateErrorStatement, deleteErrorStatement
        ).filter(Objects::nonNull).collect(Collectors.joining("; "));
        return errorStatements.isEmpty() ? null : errorStatements;
    }
}
