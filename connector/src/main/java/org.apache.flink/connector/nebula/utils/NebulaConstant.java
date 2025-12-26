/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.utils;

public class NebulaConstant {


    // Delimiter
    public static String COMMA = ",";
    public static String SUB_LINE = "_";
    public static String POINT = ".";
    public static String SPLIT_POINT = "\\.";
    public static String COLON = ":";


    // default value for read & write
    public static final int DEFAULT_SCAN_BATCH_SIZE  = 2000;
    public static final int DEFAULT_WRITE_BATCH_SIZE = 2000;
    public static final int DEFAULT_BATCH_INTERVAL_MS = 0;
    public static final int DEFAULT_VERTEX_ID_INDEX = 0;
    public static final int DEFAULT_ROW_INFO_INDEX = -1;
    public static final long DEFAULT_INTERVAL_MILLIS = 0;

    public static final int DEFAULT_RETRY_TIMES = 0;
    public static final boolean DEFAULT_ERROR_WHEN_FAILED = false;

    // default value for connection
    public static final int DEFAULT_CONNECTION_TIMEOUT_MS = 3000;
    public static final int DEFAULT_REQUEST_TIMEOUT_MS = 5000;

    // params for create space
    public static final String CREATE_VID_TYPE = "vid_type";
    public static final String CREATE_PARTITION_NUM = "partition_num";
    public static final String CREATE_REPLICA_FACTOR = "replica_factor";

    // default params for create space
    public static final int DEFAULT_PARTITION_NUM = 100;
    public static final int DEFAULT_REPLICA_FACTOR = 1;
}
