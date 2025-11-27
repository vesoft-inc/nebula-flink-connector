/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.utils;

public enum DataTypeEnum {
    NODE("NODE"),

    EDGE("EDGE");

    private String type;

    DataTypeEnum(String type) {
        this.type = type;
    }

    public boolean isNode() {
        if (NODE.type.equalsIgnoreCase(this.type)) {
            return true;
        }
        return false;
    }

    public static boolean checkValidDataType(String type) {
        if (NODE.name().equalsIgnoreCase(type) || EDGE.name().equalsIgnoreCase(type)) {
            return true;
        }
        return false;
    }
}
