/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.utils;

public enum WriteModeEnum {
    /**
     * INSERT write mode
     */
    INSERT("INSERT"),

    /**
     * INSERT OR IGNORE write mode
     */
    INSERTIGNORE("INSERT OR IGNORE"),


    /**
     * INSERT OR IGNORE write mode
     */
    INSERTREPLACE("INSERT OR REPLACE"),


    /**
     * INSERT OR IGNORE write mode
     */
    INSERTUPDATE("INSERT OR UPDATE"),

    /**
     * UPDATE write mode
     */
    UPDATE("UPDATE"),

    /**
     * DELETE write mode
     */
    DELETE("DELETE"),

    /**
     * DETACH DELETE wirte mode
     */
    DETACHDELETE("DETACH DELETE");

    private String mode;

    WriteModeEnum(String mode) {
        this.mode = mode;
    }

    public String getMode() {
        return mode;
    }
}
