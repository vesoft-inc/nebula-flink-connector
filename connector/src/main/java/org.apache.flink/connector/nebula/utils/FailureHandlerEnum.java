/* Copyright (c) 2023 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.utils;

public enum FailureHandlerEnum {
    FAIL("FAIL"),

    IGNORE("IGNORE");

    private final String handler;

    FailureHandlerEnum(String handler) {
        this.handler = handler;
    }
}
