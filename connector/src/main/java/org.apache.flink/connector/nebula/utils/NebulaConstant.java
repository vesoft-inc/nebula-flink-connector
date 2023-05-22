/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.utils;

public class NebulaConstant {
    // template for insert statement
    public static String BATCH_INSERT_TEMPLATE = "INSERT %s `%s`(%s) VALUES %s";
    public static String VERTEX_VALUE_TEMPLATE = "%s: (%s)";
    public static String VERTEX_VALUE_TEMPLATE_WITH_POLICY = "%s(\"%s\"): (%s)";
    public static String ENDPOINT_TEMPLATE = "%s(\"%s\")";
    public static String EDGE_VALUE_WITHOUT_RANKING_TEMPLATE = "%s->%s: (%s)";
    public static String EDGE_VALUE_TEMPLATE = "%s->%s@%d: (%s)";

    // template for update statement
    public static String UPDATE_VERTEX_TEMPLATE = "UPDATE %s ON `%s` %s SET %s";
    public static String UPDATE_EDGE_TEMPLATE = "UPDATE %s ON `%s` %s->%s@%d SET %s";
    public static String UPDATE_VALUE_TEMPLATE = "`%s`=%s";

    // template for delete statement
    public static String DELETE_VERTEX_TEMPLATE = "DELETE VERTEX %s";
    public static String DELETE_EDGE_TEMPLATE = "DELETE EDGE `%s` %s";
    public static String EDGE_ENDPOINT_TEMPLATE = "%s->%s@%d";

    // template for create space statement
    public static String CREATE_SPACE_TEMPLATE = "CREATE SPACE `%s` (%s)";
    public static String CREATE_SPACE_COMMENT = " COMMENT = '%s'";

    // Delimiter
    public static String COMMA = ",";
    public static String SUB_LINE = "_";
    public static String POINT = ".";
    public static String SPLIT_POINT = "\\.";
    public static String COLON = ":";


    // default value for read & write
    public static final int DEFAULT_SCAN_LIMIT = 2000;
    public static final int DEFAULT_WRITE_BATCH_SIZE = 2000;
    public static final int DEFAULT_BATCH_INTERVAL_MS = 0;
    public static final int DEFAULT_VERTEX_ID_INDEX = 0;
    public static final int DEFAULT_ROW_INFO_INDEX = -1;

    // default value for connection
    public static final int DEFAULT_TIMEOUT_MS = 1000;
    public static final int DEFAULT_CONNECT_TIMEOUT_MS = 3000;
    public static final int DEFAULT_CONNECT_RETRY = 3;
    public static final int DEFAULT_EXECUTION_RETRY = 3;
    public static final int DEFAULT_RETRY_DELAY_MS = 1000;

    // params for create space
    public static final String CREATE_VID_TYPE = "vid_type";
    public static final String CREATE_PARTITION_NUM = "partition_num";
    public static final String CREATE_REPLICA_FACTOR = "replica_factor";

    // default params for create space
    public static final int DEFAULT_PARTITION_NUM = 100;
    public static final int DEFAULT_REPLICA_FACTOR = 1;
}
