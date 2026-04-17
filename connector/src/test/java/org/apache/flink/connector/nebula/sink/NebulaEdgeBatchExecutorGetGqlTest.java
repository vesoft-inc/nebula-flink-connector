/*
 * Copyright (c) 2026 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.sink;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.connector.nebula.options.SinkEdgeOptions;
import org.apache.flink.connector.nebula.utils.NebulaEdge;
import org.apache.flink.connector.nebula.utils.NebulaEdgeSchema;
import org.apache.flink.connector.nebula.utils.WriteModeEnum;
import org.junit.Assert;
import org.junit.Test;

public class NebulaEdgeBatchExecutorGetGqlTest {

    @Test
    public void testGetGqlWithInsertMode() {
        TestableNebulaEdgeBatchExecutor executor =
                new TestableNebulaEdgeBatchExecutor(buildOptions(WriteModeEnum.INSERTREPLACE, null),
                                                    buildSchema(Collections.emptyList()));

        String gql = executor.buildGql(Collections.singletonList(buildEdge()));

        Assert.assertTrue(gql.contains("INSERT OR REPLACE"));
        Assert.assertTrue(gql.contains("@`friend`"));
        Assert.assertTrue(gql.contains("TABLE t{src_0,dst_0,c0,c1}"));
    }

    @Test
    public void testGetGqlWithUpdateModeAndMultiEdgeKeys() {
        TestableNebulaEdgeBatchExecutor executor =
                new TestableNebulaEdgeBatchExecutor(buildOptions(WriteModeEnum.UPDATE, null),
                                                    buildSchema(Collections.singletonList("rank")));

        String gql = executor.buildGql(Collections.singletonList(buildEdge()));

        Assert.assertTrue(gql.contains("SET n_e.`weight`=r.c1"));
        Assert.assertFalse(gql.contains("SET n_e.`rank`=r.c0"));
        Assert.assertTrue(gql.contains("AND n_e.`rank`=r.c0"));
    }

    @Test
    public void testGetGqlWithDeleteModeAndMultiEdgeKeys() {
        TestableNebulaEdgeBatchExecutor executor =
                new TestableNebulaEdgeBatchExecutor(buildOptions(WriteModeEnum.DELETE, null),
                                                    buildSchema(Collections.singletonList("rank")));

        String gql = executor.buildGql(Collections.singletonList(buildEdge()));

        Assert.assertTrue(gql.contains("DELETE n_e"));
        Assert.assertTrue(gql.contains("TABLE t{src_0,dst_0,c0}"));
        Assert.assertTrue(gql.contains("AND n_e.`rank`=r.c0"));
    }

    @Test
    public void testGetGqlWithTemplate() {
        String template = "{{TABLE}}\nUSE `{{GRAPH}}`\nFOR r IN t\n"
                + "INSERT (@`{{TYPE}}`{`rank`:r.c0,`weight`:r.c1})";
        TestableNebulaEdgeBatchExecutor executor =
                new TestableNebulaEdgeBatchExecutor(buildOptions(WriteModeEnum.INSERTREPLACE,
                                                                  template),
                                                    buildSchema(Collections.emptyList()));

        String gql = executor.buildGql(Collections.singletonList(buildEdge()));

        Assert.assertTrue(gql.contains("TABLE t{src_0,dst_0,c0,c1}"));
        Assert.assertTrue(gql.contains("USE `graph_test`"));
        Assert.assertTrue(gql.contains("@`friend`"));
    }

    private SinkEdgeOptions buildOptions(WriteModeEnum writeMode, String gqlTemplate) {
        SinkEdgeOptions.Builder builder = SinkEdgeOptions.builder()
                .withGraphName("graph_test")
                .withEdgeType("friend")
                .withFlinkSrcPkFields(Collections.singletonList("src"))
                .withNebulaSrcPks(Collections.singletonList("id"))
                .withFlinkDstPkFields(Collections.singletonList("dst"))
                .withNebulaDstPks(Collections.singletonList("id"))
                .withFlinkFields(Arrays.asList("f_rank", "f_weight"))
                .withNebulaFields(Arrays.asList("rank", "weight"))
                .withWriteMode(writeMode);
        if (gqlTemplate != null) {
            builder.withGqlTemplate(gqlTemplate);
        }
        return builder.build();
    }

    private NebulaEdgeSchema buildSchema(List<String> multipleEdgeKeys) {
        NebulaEdgeSchema schema = new NebulaEdgeSchema();
        schema.setEdgeTypeName("friend");
        schema.setSrcNodeTypeName("person");
        schema.setDstNodeTypeName("person");
        schema.setSrcPkNames(Collections.singletonList("id"));
        schema.setDstPkNames(Collections.singletonList("id"));

        Map<String, String> srcPkDataType = new HashMap<>();
        srcPkDataType.put("id", "STRING");
        schema.setSrcPkDataTypeMap(srcPkDataType);

        Map<String, String> dstPkDataType = new HashMap<>();
        dstPkDataType.put("id", "STRING");
        schema.setDstPkDataTypeMap(dstPkDataType);

        schema.setPropNames(Arrays.asList("rank", "weight"));
        Map<String, String> properties = new HashMap<>();
        properties.put("rank", "INT64");
        properties.put("weight", "INT32");
        schema.setProperties(properties);
        schema.setMultipleEdgeKeys(multipleEdgeKeys);
        return schema;
    }

    private NebulaEdge buildEdge() {
        Map<String, String> srcPks = new HashMap<>();
        srcPks.put("id", "\"src_1\"");

        Map<String, String> dstPks = new HashMap<>();
        dstPks.put("id", "\"dst_2\"");

        Map<String, String> props = new HashMap<>();
        props.put("rank", "1");
        props.put("weight", "10");
        return new NebulaEdge(srcPks, dstPks, props);
    }

    private static class TestableNebulaEdgeBatchExecutor extends NebulaEdgeBatchExecutor {
        TestableNebulaEdgeBatchExecutor(SinkEdgeOptions options, NebulaEdgeSchema schema) {
            super(options, schema);
        }

        String buildGql(List<NebulaEdge> edges) {
            return super.getGql(edges);
        }
    }
}

