/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.sink;

import com.vesoft.nebula.client.graph.net.Session;

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
     * @param session graph session
     */
    String executeBatch(Session session);
}
