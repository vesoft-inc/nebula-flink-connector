/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.sink;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.connector.nebula.options.SinkEdgeOptions;
import org.apache.flink.connector.nebula.options.SinkNodeOptions;
import org.apache.flink.connector.nebula.utils.NebulaEdge;
import org.apache.flink.connector.nebula.utils.NebulaEdgeSchema;
import org.apache.flink.connector.nebula.utils.NebulaNode;
import org.apache.flink.connector.nebula.utils.NebulaNodeSchema;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;

public class NebulaOutputFormatConverterTest {
    Map<String, String> nodeSchema = new HashMap<>();
    Map<String, String> edgeSchema = new HashMap<>();
    Row                 row        = Row.withNames();

    @Before
    public void before() {
        row.setField("src", 2);
        row.setField("dst", "Tom");
        row.setField("degree", 12.0);
        row.setField("date", "2020-01-01");
        row.setField("datetime", "2020-01-01 12:12:12:0000");
        row.setField("time", "12:12:12:0000");
        row.setField("name", "a");
        row.setField("age", 11);
        row.setField("aaa", 12.0);
    }

    @Test
    public void testNodeValue() {
        nodeSchema.put("id", "STRING");
        nodeSchema.put("dst", "STRING");
        nodeSchema.put("degree", "DOUBLE");
        nodeSchema.put("date", "DATE");
        nodeSchema.put("datetime", "LOCAL DATETIME");
        nodeSchema.put("time", "LOCAL TIME");
        nodeSchema.put("name", "STRING");
        nodeSchema.put("age", "INT16");
        nodeSchema.put("aaa", "DOUBLE");
        nodeSchema.put("bbb", "INT16");

        SinkNodeOptions options = SinkNodeOptions.builder()
                .withGraphName("flink_sink_node_test")
                .withNodeType("player")
                .withNebulaFields(Arrays.asList("id", "name", "age", "date", "datetime", "time"))
                .withFlinkFields(Arrays.asList("src", "name", "age", "date", "datetime", "time"))
                .build();

        NebulaNodeSchema nodeSchema = new NebulaNodeSchema();
        nodeSchema.setNodeTypeName("player");
        nodeSchema.setPkNames(Arrays.asList("id"));
        nodeSchema.setPropNames(Arrays.asList("id", "dst", "degree", "date", "time", "name",
                                              "age", "aaa", "bbb"));
        nodeSchema.setProperties(this.nodeSchema);
        NebulaRowNodeOutputFormatConverter converter =
                new NebulaRowNodeOutputFormatConverter(options, nodeSchema);

        NebulaNode node = converter.createNode(row);
        assert (node.getProperties().size() == 6);
        assert (node.getProperties().get("id").equals("\"2\""));
        assert (node.getProperties().get("name").equals("\"a\""));
        assert (node.getProperties().get("age").equals("11"));
        assert (node.getProperties().get("date").equals("date(\"2020-01-01\")"));
        assert (node.getProperties().get("datetime")
                .equals("local_datetime(\"2020-01-01 12:12:12:0000\")"));
        assert (node.getProperties().get("time").equals("local_time(\"12:12:12:0000\")"));
    }


    @Test
    public void testCreateEdgeValue() {
        edgeSchema.put("degree", "DOUBLE");
        edgeSchema.put("date", "DATE");
        edgeSchema.put("age", "INT32");
        edgeSchema.put("datetime", "LOCAL DATETIME");
        edgeSchema.put("time", "LOCAL TIME");
        edgeSchema.put("aaa", "DOUBLE");
        edgeSchema.put("bbb", "INT16");

        NebulaEdgeSchema nebulaEdgeSchema = new NebulaEdgeSchema();
        nebulaEdgeSchema.setEdgeTypeName("follow");
        nebulaEdgeSchema.setSrcPkNames(Arrays.asList("id"));
        Map<String,String> srcPkDataType = new HashMap<>();
        srcPkDataType.put("id", "STRING");
        nebulaEdgeSchema.setSrcPkDataTypeMap(srcPkDataType);
        nebulaEdgeSchema.setDstPkNames(Arrays.asList("id"));
        Map<String,String> dstPkDataType = new HashMap<>();
        dstPkDataType.put("id", "STRING");
        nebulaEdgeSchema.setDstPkDataTypeMap(dstPkDataType);
        nebulaEdgeSchema.setPropNames(Arrays.asList("degree", "date", "time", "aaa", "bbb"));
        nebulaEdgeSchema.setProperties(edgeSchema);
        SinkEdgeOptions options = SinkEdgeOptions
                .builder()
                .withGraphName("flink_sink_edge_test")
                .withEdgeType("follow")
                .withNebulaSrcPks(Arrays.asList("id"))
                .withFlinkSrcPkFields(Arrays.asList("src"))
                .withNebulaDstPks(Arrays.asList("id"))
                .withFlinkDstPkFields(Arrays.asList("dst"))
                .withNebulaFields(Arrays.asList("degree", "age", "date", "datetime", "time"))
                .withFlinkFields(Arrays.asList("degree", "age", "date", "datetime", "time"))
                .build();
        NebulaRowEdgeOutputFormatConverter converter =
                new NebulaRowEdgeOutputFormatConverter(options, nebulaEdgeSchema);

        NebulaEdge edge = converter.createEdge(row);
        assert (edge.getSrcPks().size() == 1);
        assert (edge.getDstPks().size() == 1);
        assert (edge.getSrcPks().get("id").equals("\"2\""));
        assert (edge.getDstPks().get("id").equals("\"Tom\""));
        assert (edge.getProperties().size() == 5);
        assert (edge.getProperties().get("degree").equals("12.0"));
        assert (edge.getProperties().get("date").equals("date(\"2020-01-01\")"));
        assert (edge.getProperties().get("datetime")
                .equals("local_datetime(\"2020-01-01 12:12:12:0000\")"));
        assert (edge.getProperties().get("time").equals("local_time(\"12:12:12:0000\")"));
    }

}
