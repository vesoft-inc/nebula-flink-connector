/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
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

    public static boolean checkValidWriteMode(String modeName) {
        return chooseWriteMode(modeName) != INSERT
                || INSERT.name().equalsIgnoreCase(modeName);
    }

    public static WriteModeEnum chooseWriteMode(String modeName) {
        if (UPDATE.name().equalsIgnoreCase(modeName)) {
            return UPDATE;
        }
        if (DELETE.name().equalsIgnoreCase(modeName)) {
            return DELETE;
        }
        return INSERT;
    }
}
