/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.sink;

import org.apache.flink.connector.nebula.connection.GraphProvider;

public interface NebulaBatchExecutor<T> {

    /**
     * put record into buffer
     *
     * @param record represent vertex or edge
     */
    void addToBatch(T record);

    /**
     * execute the statement
     *
     * @param graphProvider graph connection provider
     */
    String executeBatch(GraphProvider graphProvider);
}
