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
import org.apache.flink.connector.nebula.options.SinkNodeOptions;
import org.apache.flink.connector.nebula.utils.NebulaNode;
import org.apache.flink.connector.nebula.utils.NebulaNodeSchema;
import org.apache.flink.connector.nebula.utils.WriteModeEnum;
import org.junit.Assert;
import org.junit.Test;

public class NebulaNodeBatchExecutorGetGqlTest {

    @Test
    public void testGetGqlWithInsertMode() {
        TestableNebulaNodeBatchExecutor executor =
                new TestableNebulaNodeBatchExecutor(buildOptions(WriteModeEnum.INSERTREPLACE, null),
                                                    buildSchema());

        String gql = executor.buildGql(Collections.singletonList(buildNode()));

        Assert.assertTrue(gql.contains("INSERT OR REPLACE"));
        Assert.assertTrue(gql.contains("@`person`"));
        Assert.assertTrue(gql.contains("TABLE t{c0,c1,c2}"));
    }

    @Test
    public void testGetGqlWithUpdateMode() {
        TestableNebulaNodeBatchExecutor executor =
                new TestableNebulaNodeBatchExecutor(buildOptions(WriteModeEnum.UPDATE, null),
                                                    buildSchema());

        String gql = executor.buildGql(Collections.singletonList(buildNode()));

        Assert.assertTrue(gql.contains("SET n_v.name=r.c1,n_v.age=r.c2"));
        Assert.assertTrue(gql.contains("WHERE n_v.`id`=r.c0"));
    }

    @Test
    public void testGetGqlWithDeleteMode() {
        TestableNebulaNodeBatchExecutor executor =
                new TestableNebulaNodeBatchExecutor(buildOptions(WriteModeEnum.DETACHDELETE, null),
                                                    buildSchema());

        String gql = executor.buildGql(Collections.singletonList(buildNode()));

        Assert.assertTrue(gql.contains("DETACH DELETE n_v"));
        Assert.assertTrue(gql.contains("TABLE t{c0}"));
    }

    @Test
    public void testGetGqlWithTemplate() {
        String template = "{{TABLE}}\nUSE `{{GRAPH}}`\nFOR r IN t\n"
                + "INSERT (@`{{TYPE}}`{`id`:r.c0,`name`:r.c1,`age`:r.c2})";
        TestableNebulaNodeBatchExecutor executor =
                new TestableNebulaNodeBatchExecutor(buildOptions(WriteModeEnum.INSERTREPLACE,
                                                                  template),
                                                    buildSchema());

        String gql = executor.buildGql(Collections.singletonList(buildNode()));

        Assert.assertTrue(gql.contains("TABLE t{c0,c1,c2}"));
        Assert.assertTrue(gql.contains("USE `graph_test`"));
        Assert.assertTrue(gql.contains("@`person`"));
    }

    private SinkNodeOptions buildOptions(WriteModeEnum writeMode, String gqlTemplate) {
        SinkNodeOptions.Builder builder = SinkNodeOptions.builder()
                .withGraphName("graph_test")
                .withNodeType("person")
                .withFlinkFields(Arrays.asList("f_id", "f_name", "f_age"))
                .withNebulaFields(Arrays.asList("id", "name", "age"))
                .withWriteMode(writeMode);
        if (gqlTemplate != null) {
            builder.withGqlTemplate(gqlTemplate);
        }
        return builder.build();
    }

    private NebulaNodeSchema buildSchema() {
        NebulaNodeSchema schema = new NebulaNodeSchema();
        schema.setNodeTypeName("person");
        schema.setPkNames(Collections.singletonList("id"));
        schema.setPropNames(Arrays.asList("id", "name", "age"));
        Map<String, String> properties = new HashMap<>();
        properties.put("id", "STRING");
        properties.put("name", "STRING");
        properties.put("age", "INT32");
        schema.setProperties(properties);
        return schema;
    }

    private NebulaNode buildNode() {
        Map<String, String> props = new HashMap<>();
        props.put("id", "\"n1\"");
        props.put("name", "\"tom\"");
        props.put("age", "10");
        return new NebulaNode(props);
    }

    private static class TestableNebulaNodeBatchExecutor extends NebulaNodeBatchExecutor {
        TestableNebulaNodeBatchExecutor(SinkNodeOptions options, NebulaNodeSchema schema) {
            super(options, schema);
        }

        String buildGql(List<NebulaNode> nodes) {
            return super.getGql(nodes);
        }
    }
}

