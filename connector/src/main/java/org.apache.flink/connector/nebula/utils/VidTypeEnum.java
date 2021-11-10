/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.utils;

public enum VidTypeEnum {
    /**
     * INT vertex id
     */
    INT("INT"),

    /**
     * STRING vertex id
     */
    STRING("STRING");

    private String type;

    VidTypeEnum(String type) {
        this.type = type;
    }

    public String vidType() {
        return type;
    }
}
