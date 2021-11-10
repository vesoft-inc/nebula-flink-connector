/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.sink;

import com.vesoft.nebula.meta.PropertyType;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.connector.nebula.sink.NebulaRowEdgeOutputFormatConverter;
import org.apache.flink.connector.nebula.sink.NebulaRowVertexOutputFormatConverter;
import org.apache.flink.connector.nebula.statement.EdgeExecutionOptions;
import org.apache.flink.connector.nebula.statement.ExecutionOptions;
import org.apache.flink.connector.nebula.statement.VertexExecutionOptions;
import org.apache.flink.connector.nebula.utils.NebulaEdge;
import org.apache.flink.connector.nebula.utils.NebulaVertex;
import org.apache.flink.connector.nebula.utils.PolicyEnum;
import org.apache.flink.connector.nebula.utils.VidTypeEnum;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;

public class NebulaOutputFormatConverterTest {
    Map<String, Integer> schema = new HashMap<>();
    Row row = new Row(9);

    @Before
    public void before() {
        schema.put("src", PropertyType.STRING.getValue());
        schema.put("dst", PropertyType.STRING.getValue());
        schema.put("degree", PropertyType.DOUBLE.getValue());
        schema.put("date", PropertyType.DATE.getValue());
        schema.put("datetime", PropertyType.DATETIME.getValue());
        schema.put("time", PropertyType.TIME.getValue());
        schema.put("name", PropertyType.STRING.getValue());
        schema.put("age", PropertyType.INT16.getValue());
        schema.put("aaa", PropertyType.DOUBLE.getValue());
        schema.put("bbb", PropertyType.INT16.getValue());

        row.setField(0, 2);
        row.setField(1, "Tom");
        row.setField(2, "Jena");
        row.setField(3, "2020-01-01");
        row.setField(4, "2020-01-01 12:12:12:0000");
        row.setField(5, "12:12:12:0000");
        row.setField(6, "a");
        row.setField(7, 11);
        row.setField(8, 12.0);
    }

    @Test
    public void testCreateVertexValue() {
        ExecutionOptions rowInfoConfig = new VertexExecutionOptions.ExecutionOptionBuilder()
                .setGraphSpace("test")
                .setTag("tag")
                .setIdIndex(0)
                .setFields(Arrays.asList("name", "age"))
                .setPositions(Arrays.asList(1, 7))
                .builder();

        NebulaRowVertexOutputFormatConverter helper =
                new NebulaRowVertexOutputFormatConverter((VertexExecutionOptions) rowInfoConfig,
                        VidTypeEnum.STRING,
                        schema);

        NebulaVertex vertex = helper.createVertex(row, null);
        assert (vertex.getVid().equals("\"2\""));
        assert (vertex.getPropValues().size() == 2);
        assert (vertex.getPropValuesString().equals("\"Tom\",11"));
    }

    @Test
    public void testVertexDateValue() {
        ExecutionOptions rowInfoConfig = new VertexExecutionOptions.ExecutionOptionBuilder()
                .setGraphSpace("test")
                .setTag("tag")
                .setIdIndex(0)
                .setFields(Arrays.asList("name", "date", "datetime", "time", "age"))
                .setPositions(Arrays.asList(1, 3, 4, 5, 7))
                .builder();
        NebulaRowVertexOutputFormatConverter helper =
                new NebulaRowVertexOutputFormatConverter((VertexExecutionOptions) rowInfoConfig,
                        VidTypeEnum.STRING,
                        schema);

        NebulaVertex vertex = helper.createVertex(row, null);
        assert (vertex.getVid().equals("\"2\""));
        assert (vertex.getPropValuesString().equals("\"Tom\",date(\"2020-01-01\"),datetime"
                + "(\"2020-01-01 12:12:12:0000\"),time(\"12:12:12:0000\"),11"));
    }

    @Test
    public void testIntVidVertex() {
        ExecutionOptions rowInfoConfig = new VertexExecutionOptions.ExecutionOptionBuilder()
                .setGraphSpace("test")
                .setTag("tag")
                .setIdIndex(1)
                .setFields(Arrays.asList("name", "age"))
                .setPositions(Arrays.asList(1, 7))
                .builder();
        NebulaRowVertexOutputFormatConverter helper =
                new NebulaRowVertexOutputFormatConverter((VertexExecutionOptions) rowInfoConfig,
                        VidTypeEnum.INT,
                        schema);

        NebulaVertex vertex = helper.createVertex(row, PolicyEnum.HASH);
        assert (vertex.getVid().equals("Tom"));
        assert (vertex.getPropValues().size() == 2);
        assert (vertex.getPropValuesString().equals("\"Tom\",11"));
    }


    @Test
    public void testCreateEdgeValue() {
        ExecutionOptions rowInfoConfig = new EdgeExecutionOptions.ExecutionOptionBuilder()
                .setGraphSpace("test")
                .setEdge("edge")
                .setSrcIndex(1)
                .setDstIndex(2)
                .setRankIndex(0)
                .setFields(Arrays.asList("src", "dst", "degree"))
                .setPositions(Arrays.asList(1, 2, 8))
                .builder();

        NebulaRowEdgeOutputFormatConverter helper =
                new NebulaRowEdgeOutputFormatConverter((EdgeExecutionOptions) rowInfoConfig,
                        VidTypeEnum.STRING,
                        schema);
        NebulaEdge edge = helper.createEdge(row, null);
        assert (edge.getSource().equals("\"Tom\""));
        assert (edge.getTarget().equals("\"Jena\""));
        assert (edge.getRank() == 2);
        assert (edge.getPropValues().size() == 3);
        assert (edge.getPropValuesString().equals("\"Tom\",\"Jena\",12.0"));
    }


    @Test
    public void testEdgeDateValue() {
        ExecutionOptions rowInfoConfig = new EdgeExecutionOptions.ExecutionOptionBuilder()
                .setGraphSpace("test")
                .setEdge("edge")
                .setSrcIndex(1)
                .setDstIndex(2)
                .setFields(Arrays.asList("degree", "date", "datetime", "time"))
                .setPositions(Arrays.asList(8, 3, 4, 5))
                .builder();

        NebulaRowEdgeOutputFormatConverter helper =
                new NebulaRowEdgeOutputFormatConverter((EdgeExecutionOptions) rowInfoConfig,
                        VidTypeEnum.STRING,
                        schema);

        NebulaEdge edge = helper.createEdge(row, null);
        assert (edge.getSource().equals("\"Tom\""));
        assert (edge.getTarget().equals("\"Jena\""));
        assert (edge.getRank() == null);
        assert (edge.getPropValues().size() == 4);
        assert (edge.getPropValuesString().equals("12.0,date(\"2020-01-01\"),datetime"
                + "(\"2020-01-01 12:12:12:0000\"),time(\"12:12:12:0000\")"));
    }

    @Test
    public void testIntVidEdge() {
        ExecutionOptions rowInfoConfig = new EdgeExecutionOptions.ExecutionOptionBuilder()
                .setGraphSpace("test")
                .setEdge("edge")
                .setSrcIndex(1)
                .setDstIndex(2)
                .setFields(Arrays.asList("degree"))
                .setPositions(Arrays.asList(8))
                .builder();

        NebulaRowEdgeOutputFormatConverter helper =
                new NebulaRowEdgeOutputFormatConverter((EdgeExecutionOptions) rowInfoConfig,
                        VidTypeEnum.INT,
                        schema);

        NebulaEdge edge = helper.createEdge(row, PolicyEnum.HASH);
        assert (edge.getSource().equals("Tom"));
        assert (edge.getTarget().equals("Jena"));
        assert (edge.getRank() == null);
        assert (edge.getPropValues().size() == 1);
        assert (edge.getPropValuesString().equals("12.0"));
    }
}
