/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.utils;

import junit.framework.TestCase;

public class NebulaUtilsTest extends TestCase {

    public void testGetHostAndPorts() {
        assert (NebulaUtils.getHostAndPorts("127.0.0.1:9669").size() == 1);
        assert (NebulaUtils.getHostAndPorts("127.0.0.1:9669,127.0.0.1:9670").size() == 2);
        try {
            NebulaUtils.getHostAndPorts(null);
        } catch (IllegalArgumentException e) {
            assert (true);
        } catch (Exception e) {
            assert (false);
        }

        try {
            NebulaUtils.getHostAndPorts("127.0.0.1");
        } catch (IllegalArgumentException e) {
            assert (true);
        } catch (Exception e) {
            assert (false);
        }
    }

    public void testIsNumeric() {
        assert (NebulaUtils.isNumeric("123456"));
        assert (NebulaUtils.isNumeric("0123456"));
        assert (NebulaUtils.isNumeric("-123456"));
        assert (NebulaUtils.isNumeric("000"));
        assert (!NebulaUtils.isNumeric("aaa"));
        assert (!NebulaUtils.isNumeric("0123aaa"));
        assert (!NebulaUtils.isNumeric("123a8"));
    }

    public void testExtraValue() {
        assert (null == NebulaUtils.extractValue("STRING", null, null));
        assert (null == NebulaUtils.extractValue("DATE", "", null));
        assert ("\"\"".equals(NebulaUtils.extractValue("STRING", "", null)));
        assert (null == NebulaUtils.extractValue("STRING", "", ""));
        assert ("\"a\\t\\bb\"".equals(NebulaUtils.extractValue("STRING", "a\t\bb", null)));
        assert ("\"aa\\nbb\"".equals(NebulaUtils.extractValue("STRING", "aa\nbb", null)));

        assert ("1".equals(NebulaUtils.extractValue("INT32", "1", null)));
        assert ("local_datetime(\"2021-01-01T12:12:12\")".equals(
                NebulaUtils.extractValue("LOCAL DATETIME", "2021-01-01T12:12:12", null)));
        assert ("zoned_datetime(\"2021-01-01T12:12:12\")".equals(
                NebulaUtils.extractValue("ZONED DATETIME", "2021-01-01T12:12:12", null)));
        assert ("date(\"2021-01-01\")".equals(
                NebulaUtils.extractValue("DATE", "2021-01-01", null)));
        assert ("local_time(\"12:12:12\")".equals(
                NebulaUtils.extractValue("LOCAL TIME", "12:12:12", null)));
        assert ("zoned_time(\"12:12:12\")".equals(
                NebulaUtils.extractValue("ZONED TIME", "12:12:12", null)));

        assert ("LIST[\"a\",\"b\"]".equals(
                NebulaUtils.extractValue("LIST<STRING>", "[a,b]", null)));
        assert ("VECTOR<3,DOUBLE>([1.0,2.0])".equals(
                NebulaUtils.extractValue("VECTOR<3,DOUBLE>", "[1.0,2.0]", null)));

        assert ("ST_GeogFromText(\"POINT(3 4)\")".equals(
                NebulaUtils.extractValue("GEOGRAPHY<ANY>", "POINT(3 4)", null)));

        assert ("SET{1,2,3}".equals(NebulaUtils.extractValue("SET<INT32>", "{1,2,3}", null)));
        assert ("SET{\"1\",\"2\",\"3\"}".equals(NebulaUtils.extractValue("SET<STRING>",
                                                                         "{\"1\",\"2\",\"3\"}",
                                                                         null)));

        assert ("MAP{'a':123}".equals(NebulaUtils.extractValue("MAP<STRING,INT32>",
                                                               "{'a':123}", null)));
    }

    public void testMkString() {
        assertEquals("\"test\"", NebulaUtils.mkString("test", "\"", "", "\""));
        assertEquals("\"t,e,s,t\"", NebulaUtils.mkString("test", "\"", ",", "\""));
    }
}
