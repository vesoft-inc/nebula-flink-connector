package org.apache.flink.connector.nebula.sink;

import com.vesoft.nebula.meta.PropertyType;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.connector.nebula.statement.EdgeExecutionOptions;
import org.apache.flink.connector.nebula.statement.ExecutionOptions;
import org.apache.flink.connector.nebula.utils.NebulaEdge;
import org.apache.flink.connector.nebula.utils.PolicyEnum;
import org.apache.flink.connector.nebula.utils.VidTypeEnum;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NebulaRowEdgeOutputFormatConverterTest {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(NebulaRowEdgeOutputFormatConverterTest.class);

    EdgeExecutionOptions.ExecutionOptionBuilder builder = null;
    Map<String, Integer> schema = new HashMap<>();
    Row row = new Row(10);

    @Before
    public void setUp() {
        builder = new EdgeExecutionOptions.ExecutionOptionBuilder()
                .setGraphSpace("test")
                .setEdge("friend")
                .setSrcIndex(0)
                .setDstIndex(1)
                .setFields(Arrays.asList("col1", "col2", "col3", "col4", "col5", "col6", "col7",
                        "col8"))
                .setPositions(Arrays.asList(2, 3, 4, 5, 6, 7, 8, 9));

        schema.put("col1", PropertyType.STRING.getValue());
        schema.put("col2", PropertyType.FIXED_STRING.getValue());
        schema.put("col3", PropertyType.INT32.getValue());
        schema.put("col4", PropertyType.DOUBLE.getValue());
        schema.put("col5", PropertyType.DATE.getValue());
        schema.put("col6", PropertyType.DATETIME.getValue());
        schema.put("col7", PropertyType.TIME.getValue());
        schema.put("col8", PropertyType.TIMESTAMP.getValue());

        row.setField(0, 1);
        row.setField(1, 2);
        row.setField(2, "Tom");
        row.setField(3, "Tom");
        row.setField(4, 10);
        row.setField(5, 1.0);
        row.setField(6, "2021-01-01");
        row.setField(7, "2021-01-01T12:00:00");
        row.setField(8, "12:00:00");
        row.setField(9, 372435234);
    }

    public void tearDown() {
    }

    /**
     * test create edge for int id
     */
    @Test
    public void testCreateEdgeIntId() {
        ExecutionOptions options = builder.builder();
        NebulaRowEdgeOutputFormatConverter converter =
                new NebulaRowEdgeOutputFormatConverter((EdgeExecutionOptions) options,
                        VidTypeEnum.INT, schema);
        NebulaEdge edge = converter.createEdge(row, null);
        assert (edge.getSource().equals("1"));
        assert (edge.getTarget().equals("2"));
        assert (edge.getRank() == null);
        assert (edge.getPropValuesString().equals("\"Tom\",\"Tom\",10,1.0,date(\"2021-01-01\"),"
                + "datetime(\"2021-01-01T12:00:00\"),time(\"12:00:00\"),372435234"));

    }

    /**
     * test create edge with rank for int id
     */
    @Test
    public void testCreateEdgeIntIdWithRank() {
        ExecutionOptions options = builder.setRankIndex(4).builder();
        NebulaRowEdgeOutputFormatConverter converter =
                new NebulaRowEdgeOutputFormatConverter((EdgeExecutionOptions) options,
                        VidTypeEnum.INT, schema);
        NebulaEdge edge = converter.createEdge(row, null);
        assert (edge.getSource().equals("1"));
        assert (edge.getTarget().equals("2"));
        assert (edge.getRank() == 10L);
    }

    /**
     * test create edge with policy for int id
     */
    @Test
    public void testCreateEdgeIntIdWithPolicy() {
        ExecutionOptions options = builder.builder();
        NebulaRowEdgeOutputFormatConverter converter =
                new NebulaRowEdgeOutputFormatConverter((EdgeExecutionOptions) options,
                        VidTypeEnum.INT, schema);
        NebulaEdge edge = converter.createEdge(row, PolicyEnum.HASH);
        assert (edge.getSource().equals("1"));
        assert (edge.getTarget().equals("2"));
        assert (edge.getRank() == null);
    }

    /**
     * test create edge for String id
     */
    @Test
    public void testCreateEdgeStringId() {
        ExecutionOptions options = builder.builder();
        NebulaRowEdgeOutputFormatConverter converter =
                new NebulaRowEdgeOutputFormatConverter((EdgeExecutionOptions) options,
                        VidTypeEnum.STRING, schema);
        NebulaEdge edge = converter.createEdge(row, null);
        assert (edge.getSource().equals("\"1\""));
        assert (edge.getTarget().equals("\"2\""));
        assert (edge.getRank() == null);
    }

    /**
     * test create edge with rank for String id
     */
    @Test
    public void testCreateEdgeStringIdWithRank() {
        ExecutionOptions options = builder.setRankIndex(4).builder();
        NebulaRowEdgeOutputFormatConverter converter =
                new NebulaRowEdgeOutputFormatConverter((EdgeExecutionOptions) options,
                        VidTypeEnum.STRING, schema);
        NebulaEdge edge = converter.createEdge(row, null);
        assert (edge.getSource().equals("\"1\""));
        assert (edge.getTarget().equals("\"2\""));
        assert (edge.getRank() == 10L);
    }
}
