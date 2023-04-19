/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.sink;

import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.exception.IOErrorException;
import com.vesoft.nebula.client.graph.net.Session;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class NebulaBatchExecutor<T> {

    private static final Logger LOG = LoggerFactory.getLogger(NebulaBatchExecutor.class);

    /**
     * put record into buffer
     *
     * @param record represent vertex or edge
     */
    public abstract void addToBatch(T record);

    /**
     * execute the statement
     *
     * @param session graph session
     */
    public abstract void executeBatch(Session session) throws IOException;

    public abstract void clearBatch();

    public abstract boolean isBatchEmpty();

    protected static void executeStatement(Session session, String statement) throws IOException {
        LOG.debug("write statement: {}", statement);
        ResultSet execResult;
        try {
            execResult = session.execute(statement);
        } catch (IOErrorException e) {
            throw new IOException(e);
        }
        if (execResult.isSucceeded()) {
            LOG.debug("write success");
        } else {
            throw new IOException(String.format(
                    "write data failed for statement %s: %s [%s]",
                    statement, execResult.getErrorMessage(), execResult.getErrorCode()));
        }
    }
}
