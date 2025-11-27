/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.sink;

import static org.apache.flink.connector.nebula.TestConstant.sinkGraph;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.connector.nebula.options.SinkEdgeOptions;
import org.apache.flink.connector.nebula.utils.NebulaEdge;
import org.apache.flink.connector.nebula.utils.NebulaEdgeSchema;
import org.apache.flink.connector.nebula.utils.WriteModeEnum;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NebulaRowEdgeOutputFormatConverterTest {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(NebulaRowEdgeOutputFormatConverterTest.class);

    SinkEdgeOptions.Builder builder = null;
    Map<String, String>     schema  = new HashMap<>();
    Row                     row     = Row.withNames();

    @Before
    public void setUp() {
        builder = SinkEdgeOptions.builder()
                .withGraphName(sinkGraph)
                .withWriteMode(WriteModeEnum.INSERTREPLACE)
                .withEdgeType("friend")
                .withNebulaSrcPks(Arrays.asList("id"))
                .withNebulaDstPks(Arrays.asList("id"))
                .withFlinkSrcPkFields(Arrays.asList("src"))
                .withFlinkDstPkFields(Arrays.asList("dst"))
                .withNebulaFields(Arrays.asList("col1", "col2", "col3", "col4", "col5",
                                                "col6", "col7", "col8"))
                .withFlinkFields(Arrays.asList("c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8"))
                .withBatchSize(2);

        schema.put("col1", "STRING");
        schema.put("col2", "STRING");
        schema.put("col3", "INT32");
        schema.put("col4", "DOUBLE");
        schema.put("col5", "DATE");
        schema.put("col6", "LOCAL DATETIME");
        schema.put("col7", "LOCAL TIME");
        schema.put("col8", "INT64");

        row.setField("src", 1);
        row.setField("dst", 2);
        row.setField("c1", "Tom");
        row.setField("c2", "Tom");
        row.setField("c3", 10);
        row.setField("c4", 1.0);
        row.setField("c5", "2021-01-01");
        row.setField("c6", "2021-01-01T12:00:00");
        row.setField("c7", "12:00:00");
        row.setField("c8", 372435234);
    }

    public void tearDown() {
    }

    /**
     * test create edge for int id
     */
    @Test
    public void testCreateEdge() {
        SinkEdgeOptions  options    = builder.build();
        NebulaEdgeSchema edgeSchema = new NebulaEdgeSchema();
        edgeSchema.setEdgeTypeName("friend");
        edgeSchema.setSrcNodeTypeName("person");
        edgeSchema.setDstNodeTypeName("person");
        edgeSchema.setSrcPkDataTypeMap(new HashMap<String, String>() {
            {
                put("id", "STRING");
            }
        });
        edgeSchema.setDstPkDataTypeMap(new HashMap<String, String>() {
            {
                put("id", "STRING");
            }
        });
        edgeSchema.setProperties(schema);
        NebulaRowEdgeOutputFormatConverter converter =
                new NebulaRowEdgeOutputFormatConverter(options, edgeSchema);
        NebulaEdge edge = converter.createEdge(row);
        assert (edge.getSrcPks().get("id").equals("\"1\""));
        assert (edge.getDstPks().get("id").equals("\"2\""));
        assert (edge.getProperties().size() == 8);
        assert (edge.getProperties().get("col1").equals("\"Tom\""));
        assert (edge.getProperties().get("col2").equals("\"Tom\""));
        assert (edge.getProperties().get("col3").equals("10"));
        assert (edge.getProperties().get("col4").equals("1.0"));
        assert (edge.getProperties().get("col5").equals("date(\"2021-01-01\")"));
        assert (edge.getProperties().get("col6").equals("local_datetime(\"2021-01-01T12:00:00\")"));
        assert (edge.getProperties().get("col7").equals("local_time(\"12:00:00\")"));
        assert (edge.getProperties().get("col8").equals("372435234"));
    }
}
