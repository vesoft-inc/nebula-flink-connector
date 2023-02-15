/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.sink;

import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.net.Session;
import java.io.IOException;
import org.apache.flink.connector.nebula.utils.NebulaConstant;
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
    public abstract void executeBatch(Session session);

    protected static void executeStatement(Session session, String statement, int maxRetries)
            throws IOException {
        if (maxRetries < 0) {
            throw new IllegalArgumentException(
                    String.format("invalid max retries: %s", maxRetries));
        }

        // The statement will be executed at most `maxRetries + 1` times.
        for (int i = 0; i <= maxRetries; i++) {
            ResultSet execResult;
            try {
                execResult = session.execute(statement);
                if (execResult.isSucceeded()) {
                    LOG.debug("write success");
                    break;
                } else {
                    throw new IOException(String.format(
                            "write data failed for statement %s: %s [%s]",
                            statement, execResult.getErrorMessage(), execResult.getErrorCode()));
                }
            } catch (Exception e) {
                LOG.error(String.format("write data error (attempt %s)", i), e);
                if (i >= maxRetries) {
                    throw new IOException(e);
                } else if (i + 1 <= maxRetries) {
                    try {
                        int wait = NebulaConstant.EXECUTION_RETRY_WAIT_INCREMENT_MS * (i + 1);
                        Thread.sleep(wait);
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        throw new IOException("interrupted", ex);
                    }
                }
            }
        }
    }
}
