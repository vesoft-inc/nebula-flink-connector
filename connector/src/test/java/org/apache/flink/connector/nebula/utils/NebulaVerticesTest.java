/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package org.apache.flink.connector.nebula.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import junit.framework.TestCase;

public class NebulaVerticesTest extends TestCase {

    List<NebulaVertex> vertices = new ArrayList<>();
    String tagName = "person";
    List<String> propNames = Arrays.asList(
            "col_string",
            "col_fixed_string",
            "col_bool",
            "col_int",
            "col_int64",
            "col_double",
            "col_date");
    List<String> props1 = Arrays.asList("\"Tom\"", "\"Tom\"", "true", "10", "100", "1.0",
            "2021-11-12");
    List<String> props2 = Arrays.asList("\"Bob\"", "\"Bob\"", "false", "20", "200", "2.0",
            "2021-05-01");


    public void testGetInsertStatement() {
        vertices.add(new NebulaVertex("\"vid1\"", props1));
        vertices.add(new NebulaVertex("\"vid2\"", props2));

        NebulaVertices nebulaVertices = new NebulaVertices(tagName, propNames, vertices, null);
        String vertexStatement = nebulaVertices.getInsertStatement();

        String expectStatement = "INSERT VERTEX `person`(`col_string`,`col_fixed_string`,"
                + "`col_bool`,"
                + "`col_int`,`col_int64`,`col_double`,`col_date`) VALUES \"vid1\": ("
                + String.join(",", props1)
                + "),\"vid2\": (" + String.join(",", props2) + ")";
        assert (expectStatement.equals(vertexStatement));
    }


    public void testGetInsertStatementWithPolicy() {
        vertices.add(new NebulaVertex("vid1", props1));
        vertices.add(new NebulaVertex("vid2", props2));

        NebulaVertices nebulaVerticesWithPolicy = new NebulaVertices(tagName, propNames, vertices,
                PolicyEnum.HASH);
        String vertexStatementWithPolicy = nebulaVerticesWithPolicy.getInsertStatement();

        String expectStatementWithPolicy = "INSERT VERTEX `person`(`col_string`,"
                + "`col_fixed_string`,`col_bool`,"
                + "`col_int`,`col_int64`,`col_double`,`col_date`) VALUES HASH(\"vid1\"): ("
                + String.join(",", props1)
                + "),HASH(\"vid2\"): (" + String.join(",", props2) + ")";
        assert (expectStatementWithPolicy.equals(vertexStatementWithPolicy));
    }

    public void testGetUpdateStatement() {
        vertices.add(new NebulaVertex("\"vid1\"", props1));
        vertices.add(new NebulaVertex("\"vid2\"", props2));

        NebulaVertices nebulaVertices = new NebulaVertices(tagName, propNames, vertices, null);
        String vertexStatement = nebulaVertices.getUpdateStatement();
        String expectStatement = "UPDATE VERTEX ON `person` \"vid1\" SET `col_string`=\"Tom\","
                + "`col_fixed_string`=\"Tom\",`col_bool`=true,`col_int`=10,`col_int64`=100,"
                + "`col_double`=1.0,`col_date`=2021-11-12;"
                + "UPDATE VERTEX ON `person` \"vid2\" SET `col_string`=\"Bob\","
                + "`col_fixed_string`=\"Bob\",`col_bool`=false,`col_int`=20,`col_int64`=200,"
                + "`col_double`=2.0,`col_date`=2021-05-01";
        assert (vertexStatement.equals(expectStatement));
    }

    public void testGetUpdateStatementWithPolicy() {
        vertices.add(new NebulaVertex("vid1", props1));
        vertices.add(new NebulaVertex("vid2", props2));

        NebulaVertices nebulaVertices = new NebulaVertices(tagName, propNames, vertices,
                PolicyEnum.HASH);
        String vertexStatement = nebulaVertices.getUpdateStatement();
        String expectStatement = "UPDATE VERTEX ON `person` HASH(\"vid1\") SET "
                + "`col_string`=\"Tom\",`col_fixed_string`=\"Tom\",`col_bool`=true,`col_int`=10,"
                + "`col_int64`=100,`col_double`=1.0,`col_date`=2021-11-12;"
                + "UPDATE VERTEX ON `person` HASH(\"vid2\") SET `col_string`=\"Bob\","
                + "`col_fixed_string`=\"Bob\",`col_bool`=false,`col_int`=20,`col_int64`=200,"
                + "`col_double`=2.0,`col_date`=2021-05-01";
        assert (vertexStatement.equals(expectStatement));
    }

    public void testGetDeleteStatement() {
        vertices.add(new NebulaVertex("\"vid1\"", props1));
        vertices.add(new NebulaVertex("\"vid2\"", props2));

        NebulaVertices nebulaVertices = new NebulaVertices(tagName, propNames, vertices, null);
        String vertexStatement = nebulaVertices.getDeleteStatement();
        String expectStatement = "DELETE VERTEX \"vid1\",\"vid2\"";
        assert (vertexStatement.equals(expectStatement));
    }

    public void testGetDeleteStatementWithPolicy() {
        vertices.add(new NebulaVertex("vid1", props1));
        vertices.add(new NebulaVertex("vid2", props2));

        NebulaVertices nebulaVertices = new NebulaVertices(tagName, propNames, vertices,
                PolicyEnum.HASH);
        String vertexStatement = nebulaVertices.getDeleteStatement();
        String expectStatement = "DELETE VERTEX HASH(\"vid1\"),HASH(\"vid2\")";
        assert (vertexStatement.equals(expectStatement));
    }

}
