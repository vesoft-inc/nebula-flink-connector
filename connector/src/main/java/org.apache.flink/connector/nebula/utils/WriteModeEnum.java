/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package org.apache.flink.connector.nebula.utils;

public enum WriteModeEnum {
    /**
     * INSERT write mode
     */
    INSERT("insert"),

    /**
     * UPDATE write mode
     */
    UPDATE("update"),

    /**
     * DELETE write mode
     */
    DELETE("delete");

    private String mode;

    WriteModeEnum(String mode) {
        this.mode = mode;
    }
}
