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

public class NebulaNodeTest extends TestCase {

    List<NebulaNode> nodes    = new ArrayList<>();
    String           nodeType = "person";

    List<String>        propNames = Arrays.asList("col_string", "col_bool", "col_int", "col_int64",
                                                  "col_double", "col_date");
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
        nodes.add(new NebulaNode(props1));
        nodes.add(new NebulaNode(props2));

        NebulaNodeSchema nodeSchema = new NebulaNodeSchema();
        nodeSchema.setNodeTypeName(nodeType);
        nodeSchema.setPropNames(Arrays.asList("col_string", "col_bool", "col_int", "col_int64",
                                              "col_double", "col_date"));
        nodeSchema.setProperties(schema);

        NebulaNodes  nebulaVertices = new NebulaNodes(nodeSchema, nodes);
        List<String> flinkFields    = Arrays.asList("c1", "c2", "c3", "c4", "c5", "c6");
        List<String> nebulaFields = Arrays.asList("col_string", "col_bool", "col_int",
                                                  "col_int64", "col_double", "col_date");
        String vertexStatement = nebulaVertices.getInsertStatement("test",
                                                                   WriteModeEnum.INSERTIGNORE,
                                                                   flinkFields,
                                                                   nebulaFields);

        String expectStatement = "TABLE t{c0,c1,c2,c3,c4,c5} = \n"
                + "(\"Tom\",false,10,100,1.0,DATE(\"2021-11-12\")),"
                + "(\"Jina\",true,20,200,2.0,DATE(\"2022-11-12\")) \n"
                + "USE `test` \n"
                + "FOR r IN t \n"
                + "INSERT OR IGNORE (@`person`{`col_string`:r.c0,`col_bool`:r.c1,`col_int`:r.c2,"
                + "`col_int64`:r.c3,`col_double`:r.c4,`col_date`:r.c5})";
        assert (expectStatement.equals(vertexStatement));
    }
}
