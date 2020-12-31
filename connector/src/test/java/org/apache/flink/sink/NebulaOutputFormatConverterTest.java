/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package org.apache.flink.sink;

import com.vesoft.nebula.meta.PropertyType;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.connector.nebula.sink.NebulaRowEdgeOutputFormatConverter;
import org.apache.flink.connector.nebula.sink.NebulaRowVertexOutputFormatConverter;
import org.apache.flink.connector.nebula.statement.EdgeExecutionOptions;
import org.apache.flink.connector.nebula.statement.ExecutionOptions;
import org.apache.flink.connector.nebula.statement.VertexExecutionOptions;
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
        schema.put("src", PropertyType.STRING);
        schema.put("dst", PropertyType.STRING);
        schema.put("degree", PropertyType.DOUBLE);
        schema.put("date", PropertyType.DATE);
        schema.put("datetime", PropertyType.DATETIME);
        schema.put("time", PropertyType.TIME);
        schema.put("name", PropertyType.STRING);
        schema.put("age", PropertyType.INT16);
        schema.put("aaa", PropertyType.DOUBLE);
        schema.put("bbb", PropertyType.INT16);

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

        String value = helper.createValue(row, null);
        assert "\"2\": (\"Tom\",11)".equals(value);
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

        String value = helper.createValue(row, null);
        assert (("\"2\": (\"Tom\",date(\"2020-01-01\"),datetime(\"2020-01-01 12:12:12:0000\"),"
                + "time(\"12:12:12:0000\"),11)").equals(value));
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

        String value = helper.createValue(row, PolicyEnum.HASH);
        assert ("HASH(\"Tom\"): (\"Tom\",11)".equals(value));
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
        String value = helper.createValue(row, null);
        assert ("\"Tom\"->\"Jena\"@2: (\"Tom\",\"Jena\",12.0)".equals(value));
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

        String value = helper.createValue(row, null);
        assert (("\"Tom\"->\"Jena\": (12.0,date(\"2020-01-01\"),datetime(\"2020-01-01 "
                + "12:12:12:0000\"),time(\"12:12:12:0000\"))").equals(value));
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

        String value = helper.createValue(row, PolicyEnum.HASH);
        assert ("HASH(\"Tom\")->HASH(\"Jena\"): (12.0)".equals(value));
    }
}
