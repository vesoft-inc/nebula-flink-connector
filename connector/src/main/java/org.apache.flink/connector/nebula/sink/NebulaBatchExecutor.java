/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.sink;

import com.vesoft.nebula.ErrorCode;
import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.exception.IOErrorException;
import com.vesoft.nebula.client.graph.net.Session;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class NebulaBatchExecutor<T> {

    public static class ExecutionException extends IOException {
        private final String statement;
        private final String errorMessage;
        private final int errorCode;

        public ExecutionException(String statement, String errorMessage, int errorCode) {
            this.statement = statement;
            this.errorMessage = errorMessage;
            this.errorCode = errorCode;
        }

        @Override
        public String getMessage() {
            return String.format("failed to execute statement %s: %s [%s]",
                    statement, errorMessage, errorCode);
        }

        public boolean isNonRecoverableError() {
            return this.errorCode == ErrorCode.E_SEMANTIC_ERROR.getValue()
                    || this.errorCode == ErrorCode.E_SYNTAX_ERROR.getValue();
        }
    }

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
            throw new ExecutionException(
                    statement, execResult.getErrorMessage(), execResult.getErrorCode());
        }
    }
}
