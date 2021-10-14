/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package org.apache.flink.connector.nebula.sink;

import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.net.Session;
import java.util.Map;
import org.apache.flink.connector.nebula.statement.EdgeExecutionOptions;
import org.apache.flink.connector.nebula.statement.ExecutionOptions;
import org.apache.flink.connector.nebula.statement.VertexExecutionOptions;
import org.apache.flink.connector.nebula.utils.NebulaConstant;
import org.apache.flink.connector.nebula.utils.VidTypeEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class NebulaBatchExecutor<T> {

    protected final ExecutionOptions executionOptions;
    protected final Map<String, Integer> schema;
    protected final VidTypeEnum vidType;

    public NebulaBatchExecutor(ExecutionOptions executionOptions,
                               VidTypeEnum vidType, Map<String, Integer> schema) {
        this.executionOptions = executionOptions;
        this.vidType = vidType;
        this.schema = schema;
    }

    /**
     * put record into buffer
     *
     * @param record represent vertex or edge
     */
    abstract void addToBatch(T record);

    /**
     * execute the insert statement
     *
     * @param session graph session
     */
    abstract String executeBatch(Session session);
}
