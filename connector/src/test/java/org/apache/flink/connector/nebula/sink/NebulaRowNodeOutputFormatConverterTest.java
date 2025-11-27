/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.sink;

import static org.apache.flink.connector.nebula.TestConstant.sinkGraph;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.connector.nebula.options.SinkNodeOptions;
import org.apache.flink.connector.nebula.utils.NebulaNode;
import org.apache.flink.connector.nebula.utils.NebulaNodeSchema;
import org.apache.flink.connector.nebula.utils.WriteModeEnum;
import org.apache.flink.types.Row;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NebulaRowNodeOutputFormatConverterTest {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(NebulaRowNodeOutputFormatConverterTest.class);

    SinkNodeOptions.Builder builder = null;
    Map<String, String>                   schema  = new HashMap<>();
    Row row = Row.withNames();

    @Before
    public void setUp() {
        builder = new SinkNodeOptions.Builder()
                .withGraphName(sinkGraph)
                .withNodeType("person")
                .withWriteMode(WriteModeEnum.INSERTREPLACE)
                .withNebulaFields(Arrays.asList("col1", "col2", "col3", "col4", "col5", "col6",
                                                "col7", "col8"))
                .withFlinkFields(Arrays.asList("c1","c2","c3","c4","c5","c6","c7","c8"));


        schema.put("col1", "STRING");
        schema.put("col2", "STRING");
        schema.put("col3", "INT32");
        schema.put("col4", "DOUBLE");
        schema.put("col5", "DATE");
        schema.put("col6", "ZONED DATETIME");
        schema.put("col7", "ZONED TIME");
        schema.put("col8", "INT64");

        row.setField("c1", 1);
        row.setField("c2", "Tom");
        row.setField("c3", 10);
        row.setField("c4", 1.0);
        row.setField("c5", "2021-01-01");
        row.setField("c6", "2021-01-01T12:00:00");
        row.setField("c7", "12:00:00");
        row.setField("c8", 372435234);
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testCreateVertex() {
        SinkNodeOptions options = builder.build();
        NebulaNodeSchema nodeSchema = new NebulaNodeSchema();
        nodeSchema.setNodeTypeName("person");
        nodeSchema.setPkNames(Collections.singletonList("col1"));
        nodeSchema.setPropNames(Arrays.asList("col1", "col2", "col3", "col4", "col5", "col6",
                                              "col7", "col8"));
        nodeSchema.setProperties(schema);
        NebulaRowNodeOutputFormatConverter converter =
                new NebulaRowNodeOutputFormatConverter(options,  nodeSchema);
        NebulaNode node = converter.createNode(row);

        assert (node.getProperties().size() == 8);
        assert (node.getProperties().get("col1").equals("\"1\""));
        assert (node.getProperties().get("col2").equals("\"Tom\""));
        assert (node.getProperties().get("col3").equals("10"));
        assert (node.getProperties().get("col4").equals("1.0"));
        assert (node.getProperties().get("col5").equals("date(\"2021-01-01\")"));
        assert (node.getProperties().get("col6").equals("zoned_datetime(\"2021-01-01T12:00:00\")"));
        assert (node.getProperties().get("col7").equals("zoned_time(\"12:00:00\")"));
        assert (node.getProperties().get("col8").equals("372435234"));
    }
}
