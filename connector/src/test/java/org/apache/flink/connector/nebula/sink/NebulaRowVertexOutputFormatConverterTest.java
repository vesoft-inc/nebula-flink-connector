package org.apache.flink.connector.nebula.sink;

import com.vesoft.nebula.meta.PropertyType;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.connector.nebula.statement.ExecutionOptions;
import org.apache.flink.connector.nebula.statement.VertexExecutionOptions;
import org.apache.flink.connector.nebula.utils.NebulaVertex;
import org.apache.flink.connector.nebula.utils.VidTypeEnum;
import org.apache.flink.types.Row;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NebulaRowVertexOutputFormatConverterTest {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(NebulaRowVertexOutputFormatConverterTest.class);

    VertexExecutionOptions.ExecutionOptionBuilder builder = null;
    Map<String, Integer> schema = new HashMap<>();
    Row row = new Row(9);

    @Before
    public void setUp() {
        builder = new VertexExecutionOptions.ExecutionOptionBuilder()
                .setGraphSpace("test")
                .setTag("person")
                .setIdIndex(0)
                .setFields(Arrays.asList("col1", "col2", "col3", "col4", "col5", "col6", "col7",
                        "col8"))
                .setPositions(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8));

        schema.put("col1", PropertyType.STRING.getValue());
        schema.put("col2", PropertyType.FIXED_STRING.getValue());
        schema.put("col3", PropertyType.INT32.getValue());
        schema.put("col4", PropertyType.DOUBLE.getValue());
        schema.put("col5", PropertyType.DATE.getValue());
        schema.put("col6", PropertyType.DATETIME.getValue());
        schema.put("col7", PropertyType.TIME.getValue());
        schema.put("col8", PropertyType.TIMESTAMP.getValue());

        row.setField(0, 1);
        row.setField(1, "Tom");
        row.setField(2, "Tom");
        row.setField(3, 10);
        row.setField(4, 1.0);
        row.setField(5, "2021-01-01");
        row.setField(6, "2021-01-01T12:00:00");
        row.setField(7, "12:00:00");
        row.setField(8, 372435234);
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testCreateVertex() {
        ExecutionOptions options = builder.builder();
        NebulaRowVertexOutputFormatConverter converter =
                new NebulaRowVertexOutputFormatConverter((VertexExecutionOptions) options,
                        VidTypeEnum.INT, schema);
        NebulaVertex vertex = converter.createVertex(row, null);
        assert (vertex.getVid().equals("1"));
        assert (vertex.getPropValuesString().equals("\"Tom\",\"Tom\",10,1.0,date(\"2021-01-01\"),"
                + "datetime(\"2021-01-01T12:00:00\"),time(\"12:00:00\"),372435234"));
    }

    /**
     * test create vertex with policy for int vid type
     */
    @Test
    public void testCreateVertexPolicy() {
        ExecutionOptions options = builder.setPolicy("HASH").builder();
        NebulaRowVertexOutputFormatConverter converter =
                new NebulaRowVertexOutputFormatConverter((VertexExecutionOptions) options,
                        VidTypeEnum.INT, schema);
        NebulaVertex vertex = converter.createVertex(row, null);
        assert (vertex.getVid().equals("1"));
        assert (vertex.getPropValuesString().equals("\"Tom\",\"Tom\",10,1.0,date(\"2021-01-01\"),"
                + "datetime(\"2021-01-01T12:00:00\"),time(\"12:00:00\"),372435234"));
    }

    /**
     * test create vertex for string vid type
     */
    @Test
    public void testCreateVertexStringId() {
        ExecutionOptions options = builder.builder();
        NebulaRowVertexOutputFormatConverter converter =
                new NebulaRowVertexOutputFormatConverter((VertexExecutionOptions) options,
                        VidTypeEnum.STRING, schema);
        NebulaVertex vertex = converter.createVertex(row, null);
        assert (vertex.getVid().equals("\"1\""));
        assert (vertex.getPropValuesString().equals("\"Tom\",\"Tom\",10,1.0,date(\"2021-01-01\"),"
                + "datetime(\"2021-01-01T12:00:00\"),time(\"12:00:00\"),372435234"));
    }
}
