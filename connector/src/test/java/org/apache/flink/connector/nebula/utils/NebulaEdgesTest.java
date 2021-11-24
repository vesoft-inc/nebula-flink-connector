/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import junit.framework.TestCase;

public class NebulaEdgesTest extends TestCase {

    List<NebulaEdge> edges = new ArrayList<>();
    String edgeName = "friend";
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
        edges.add(new NebulaEdge("\"vid1\"", "\"vid2\"", null, props1));
        edges.add(new NebulaEdge("\"vid2\"", "\"vid1\"", null, props2));
        NebulaEdges nebulaEdges = new NebulaEdges(edgeName, propNames, edges, null, null);
        String edgeStatement = nebulaEdges.getInsertStatement();

        String expectStatement =
                "INSERT EDGE `friend`(`col_string`,`col_fixed_string`,`col_bool`,`col_int`,"
                        + "`col_int64`,`col_double`,`col_date`) VALUES "
                        + "\"vid1\"->\"vid2\": (" + String.join(",", props1) + "),"
                        + "\"vid2\"->\"vid1\": (" + String.join(",", props2) + ")";
        assert (edgeStatement.equals(expectStatement));
    }

    public void testGetInsertStatementWithRank() {
        edges.add(new NebulaEdge("\"vid1\"", "\"vid2\"", 1L, props1));
        edges.add(new NebulaEdge("\"vid2\"", "\"vid1\"", 2L, props2));
        NebulaEdges nebulaEdges = new NebulaEdges(edgeName, propNames, edges, null, null);
        String edgeStatement = nebulaEdges.getInsertStatement();

        String expectStatement =
                "INSERT EDGE `friend`(`col_string`,`col_fixed_string`,`col_bool`,`col_int`,"
                        + "`col_int64`,`col_double`,`col_date`) VALUES "
                        + "\"vid1\"->\"vid2\"@1: (" + String.join(",", props1) + "),"
                        + "\"vid2\"->\"vid1\"@2: (" + String.join(",", props2) + ")";
        assert (edgeStatement.equals(expectStatement));
    }

    public void testGetInsertStatementWithPolicy() {
        edges.add(new NebulaEdge("vid1", "vid2", null, props1));
        edges.add(new NebulaEdge("vid2", "vid1", null, props2));
        NebulaEdges nebulaEdges = new NebulaEdges(edgeName, propNames, edges, PolicyEnum.HASH,
                PolicyEnum.HASH);
        String edgeStatement = nebulaEdges.getInsertStatement();

        String expectStatement =
                "INSERT EDGE `friend`(`col_string`,`col_fixed_string`,`col_bool`,`col_int`,"
                        + "`col_int64`,`col_double`,`col_date`) VALUES "
                        + "HASH(\"vid1\")->HASH(\"vid2\"): (" + String.join(",", props1) + "),"
                        + "HASH(\"vid2\")->HASH(\"vid1\"): (" + String.join(",", props2) + ")";
        assert (edgeStatement.equals(expectStatement));
    }

    public void testGetInsertStatementWithRankAndPolicy() {
        edges.add(new NebulaEdge("vid1", "vid2", 1L, props1));
        edges.add(new NebulaEdge("vid2", "vid1", 2L, props2));
        NebulaEdges nebulaEdges = new NebulaEdges(edgeName, propNames, edges, PolicyEnum.HASH,
                PolicyEnum.HASH);
        String edgeStatement = nebulaEdges.getInsertStatement();

        String expectStatement =
                "INSERT EDGE `friend`(`col_string`,`col_fixed_string`,`col_bool`,`col_int`,"
                        + "`col_int64`,`col_double`,`col_date`) VALUES "
                        + "HASH(\"vid1\")->HASH(\"vid2\")@1: (" + String.join(",", props1) + "),"
                        + "HASH(\"vid2\")->HASH(\"vid1\")@2: (" + String.join(",", props2) + ")";
        assert (edgeStatement.equals(expectStatement));
    }

    public void testGetUpdateStatement() {
        edges.add(new NebulaEdge("\"vid1\"", "\"vid2\"", null, props1));
        edges.add(new NebulaEdge("\"vid2\"", "\"vid1\"", null, props2));
        NebulaEdges nebulaEdges = new NebulaEdges(edgeName, propNames, edges, null, null);
        String edgeStatement = nebulaEdges.getUpdateStatement();

        String expectStatement =
                "UPDATE EDGE ON `friend` \"vid1\"->\"vid2\"@0 SET `col_string`=\"Tom\","
                        + "`col_fixed_string`=\"Tom\",`col_bool`=true,`col_int`=10,`col_int64`=100,"
                        + "`col_double`=1.0,`col_date`=2021-11-12;"
                        + "UPDATE EDGE ON `friend` \"vid2\"->\"vid1\"@0 SET `col_string`=\"Bob\","
                        + "`col_fixed_string`=\"Bob\",`col_bool`=false,`col_int`=20,"
                        + "`col_int64`=200,"
                        + "`col_double`=2.0,`col_date`=2021-05-01";
        assert (edgeStatement.equals(expectStatement));
    }

    public void testGetUpdateStatementWithRank() {
        edges.add(new NebulaEdge("\"vid1\"", "\"vid2\"", 1L, props1));
        edges.add(new NebulaEdge("\"vid2\"", "\"vid1\"", 2L, props2));
        NebulaEdges nebulaEdges = new NebulaEdges(edgeName, propNames, edges, null, null);
        String edgeStatement = nebulaEdges.getUpdateStatement();

        String expectStatement =
                "UPDATE EDGE ON `friend` \"vid1\"->\"vid2\"@1 SET `col_string`=\"Tom\","
                        + "`col_fixed_string`=\"Tom\",`col_bool`=true,`col_int`=10,`col_int64`=100,"
                        + "`col_double`=1.0,`col_date`=2021-11-12;"
                        + "UPDATE EDGE ON `friend` \"vid2\"->\"vid1\"@2 SET `col_string`=\"Bob\","
                        + "`col_fixed_string`=\"Bob\",`col_bool`=false,`col_int`=20,"
                        + "`col_int64`=200,`col_double`=2.0,`col_date`=2021-05-01";
        assert (edgeStatement.equals(expectStatement));
    }

    public void testGetUpdateStatementWithPolicy() {
        edges.add(new NebulaEdge("vid1", "vid2", null, props1));
        edges.add(new NebulaEdge("vid2", "vid1", null, props2));
        NebulaEdges nebulaEdges = new NebulaEdges(edgeName, propNames, edges, PolicyEnum.HASH,
                PolicyEnum.HASH);
        String edgeStatement = nebulaEdges.getUpdateStatement();

        String expectStatement =
                "UPDATE EDGE ON `friend` HASH(\"vid1\")->HASH(\"vid2\")@0 SET "
                        + "`col_string`=\"Tom\","
                        + "`col_fixed_string`=\"Tom\",`col_bool`=true,`col_int`=10,`col_int64`=100,"
                        + "`col_double`=1.0,`col_date`=2021-11-12;"
                        + "UPDATE EDGE ON `friend` HASH(\"vid2\")->HASH(\"vid1\")@0 SET "
                        + "`col_string`=\"Bob\","
                        + "`col_fixed_string`=\"Bob\",`col_bool`=false,`col_int`=20,"
                        + "`col_int64`=200,`col_double`=2.0,`col_date`=2021-05-01";
        assert (edgeStatement.equals(expectStatement));
    }


    public void testGetUpdateStatementWithRankAndPolicy() {
        edges.add(new NebulaEdge("vid1", "vid2", 1L, props1));
        edges.add(new NebulaEdge("vid2", "vid1", 2L, props2));
        NebulaEdges nebulaEdges = new NebulaEdges(edgeName, propNames, edges, PolicyEnum.HASH,
                PolicyEnum.HASH);
        String edgeStatement = nebulaEdges.getUpdateStatement();

        String expectStatement =
                "UPDATE EDGE ON `friend` HASH(\"vid1\")->HASH(\"vid2\")@1 SET "
                        + "`col_string`=\"Tom\","
                        + "`col_fixed_string`=\"Tom\",`col_bool`=true,`col_int`=10,`col_int64`=100,"
                        + "`col_double`=1.0,`col_date`=2021-11-12;"
                        + "UPDATE EDGE ON `friend` HASH(\"vid2\")->HASH(\"vid1\")@2 SET "
                        + "`col_string`=\"Bob\","
                        + "`col_fixed_string`=\"Bob\",`col_bool`=false,`col_int`=20,"
                        + "`col_int64`=200,`col_double`=2.0,`col_date`=2021-05-01";
        assert (edgeStatement.equals(expectStatement));
    }
}
