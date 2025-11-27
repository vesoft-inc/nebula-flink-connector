/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import junit.framework.TestCase;

public class NebulaEdgesTest extends TestCase {

    List<NebulaEdge>    edges     = new ArrayList<>();
    String              edgeName  = "friend";
    List<String>        propNames = Arrays.asList(
            "col_string",
            "col_bool",
            "col_int",
            "col_int64",
            "col_double",
            "col_date");
    Map<String, String> props1    = new HashMap<>();
    Map<String, String> props2    = new HashMap<>();

    Map<String, String> schema = new HashMap<String, String>() {
        {
            put("col_string", "STRING");
            put("col_bool", "BOOL");
            put("col_int", "INT32");
            put("col_int64", "INT64");
            put("col_double", "DOUBLE");
            put("col_date", "DATE");
        }
    };

    public void testGetInsertStatement() {
        props1.put("col_string", "\"Tom\"");
        props1.put("col_bool", "false");
        props1.put("col_int", "10");
        props1.put("col_int64", "100");
        props1.put("col_double", "1.0");
        props1.put("col_date", "DATE(\"2021-11-12\")");

        props2.put("col_string", "\"Jina\"");
        props2.put("col_bool", "true");
        props2.put("col_int", "20");
        props2.put("col_int64", "200");
        props2.put("col_double", "2.0");
        props2.put("col_date", "DATE(\"2022-11-12\")");

        Map<String, String> src = new HashMap<>();
        src.put("id", "\"vid1\"");
        Map<String, String> dst = new HashMap<>();
        dst.put("id", "\"vid2\"");
        edges.add(new NebulaEdge(src, dst, props1));
        edges.add(new NebulaEdge(dst, src, props2));

        Map<String, String> pkDataType = new HashMap<>();
        pkDataType.put("id", "STRING");
        NebulaEdgeSchema edgeSchema = new NebulaEdgeSchema();
        edgeSchema.setEdgeTypeName(edgeName);
        edgeSchema.setSrcNodeTypeName("person");
        edgeSchema.setDstNodeTypeName("person");
        edgeSchema.setSrcPkDataTypeMap(pkDataType);
        edgeSchema.setDstPkDataTypeMap(pkDataType);
        edgeSchema.setSrcPkNames(Arrays.asList("id"));
        edgeSchema.setDstPkNames(Arrays.asList("id"));
        edgeSchema.setPropNames(Arrays.asList("col_string", "col_bool", "col_int", "col_int64",
                                              "col_double", "col_date"));
        edgeSchema.setProperties(schema);

        NebulaEdges nebulaEdges = new NebulaEdges(edgeSchema, edges);

        List<String> flinkFields = Arrays.asList("c1", "c2", "c3", "c4", "c5", "c6");
        List<String> nebulaFields = Arrays.asList("col_string", "col_bool", "col_int",
                                                  "col_int64", "col_double", "col_date");
        List<String> flinkSrcFields = Arrays.asList("src");
        List<String> nebulaSrcPk    = Arrays.asList("id");
        List<String> flinkDstFields = Arrays.asList("dst");
        List<String> nebulaDstPk    = Arrays.asList("id");
        String edgeStatement = nebulaEdges.getInsertStatement("test",
                                                              WriteModeEnum.INSERTIGNORE,
                                                              flinkSrcFields,
                                                              nebulaSrcPk,
                                                              flinkDstFields,
                                                              nebulaDstPk,
                                                              flinkFields,
                                                              nebulaFields);

        String expectStatement =
                "TABLE t{src_0,dst_0,c0,c1,c2,c3,c4,c5} = \n"
                        + "(\"vid1\",\"vid2\",\"Tom\",false,10,100,1.0,DATE(\"2021-11-12\")),"
                        + "(\"vid2\",\"vid1\",\"Jina\",true,20,200,2.0,DATE(\"2022-11-12\")) \n"
                        + "USE `test` \n"
                        + "FOR r IN t \n"
                        + "OPTIONAL MATCH (n_src@`person`) WHERE n_src.`id`=r.src_0 "
                        + "OPTIONAL MATCH (n_dst@`person`) WHERE n_dst.`id`=r.dst_0 \n"
                        + "INSERT OR IGNORE (n_src)-[@`friend`{`col_string`:r.c0,`col_bool`:r.c1,"
                        + "`col_int`:r.c2,`col_int64`:r.c3,`col_double`:r.c4,`col_date`:r.c5}]"
                        + "->(n_dst)";

        assert (edgeStatement.equals(expectStatement));
    }
}
