/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula;

public class MockData {

    public static String createStringSpace() {
        return "CLEAR SPACE IF EXISTS `test_string`;"
                + "CREATE SPACE IF NOT EXISTS test_string(partition_num=10,"
                + "vid_type=fixed_string(8));"
                + "USE test_string;"
                + "CREATE TAG IF NOT EXISTS person(col1 fixed_string(8), col2 string, col3 int32,"
                + " col4 double, col5 date, col6 datetime, col7 time, col8 timestamp);"
                + "CREATE EDGE IF NOT EXISTS friend(col1 fixed_string(8), col2 string, col3 "
                + "int32, col4 double, col5 date, col6 datetime, col7 time, col8 timestamp);";
    }

    public static String createIntSpace() {
        return "CLEAR SPACE IF EXISTS `test_int`;"
                + "CREATE SPACE IF NOT EXISTS test_int(partition_num=10,vid_type=int64);"
                + "USE test_int;"
                + "CREATE TAG IF NOT EXISTS person(col1 fixed_string(8), col2 string, col3 int32,"
                + " col4 double, col5 date, col6 datetime, col7 time, col8 timestamp);"
                + "CREATE EDGE IF NOT EXISTS friend(col1 fixed_string(8), col2 string, col3 "
                + "int32, col4 double, col5 date, col6 datetime, col7 time, col8 timestamp);";
    }

    public static String createFlinkSinkSpace() {
        return "CLEAR SPACE IF EXISTS `flink_sink`;"
                + "CREATE SPACE IF NOT EXISTS flink_sink(partition_num=10,"
                + "vid_type=fixed_string(8));"
                + "USE flink_sink;"
                + "CREATE TAG IF NOT EXISTS player(name string, age int);"
                + "CREATE EDGE IF NOT EXISTS follow(degree int);";
    }

    public static String createFlinkTestSpace() {
        return "CLEAR SPACE IF EXISTS `flink_test`;"
                + " CREATE SPACE IF NOT EXISTS `flink_test` (partition_num = 100,"
                + " charset = utf8, replica_factor = 3, collate = utf8_bin, vid_type = INT64);"
                + " USE `flink_test`;"
                + " CREATE TAG IF NOT EXISTS person (col1 string, col2 fixed_string(8),"
                + " col3 int8, col4 int16, col5 int32, col6 int64,"
                + " col7 date, col8 datetime, col9 timestamp, col10 bool,"
                + " col11 double, col12 float, col13 time, col14 geography);"
                + " CREATE EDGE IF NOT EXISTS friend (col1 string, col2 fixed_string(8),"
                + " col3 int8, col4 int16, col5 int32, col6 int64,"
                + " col7 date, col8 datetime, col9 timestamp, col10 bool,"
                + " col11 double, col12 float, col13 time, col14 geography);";
    }
}
