/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.sink;

import com.facebook.thrift.TException;
import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.exception.AuthFailedException;
import com.vesoft.nebula.client.graph.exception.ClientServerIncompatibleException;
import com.vesoft.nebula.client.graph.exception.IOErrorException;
import com.vesoft.nebula.client.graph.exception.NotValidConnectionException;
import com.vesoft.nebula.client.graph.net.NebulaPool;
import com.vesoft.nebula.client.graph.net.Session;
import com.vesoft.nebula.client.meta.MetaClient;
import java.io.Flushable;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.nebula.connection.NebulaGraphConnectionProvider;
import org.apache.flink.connector.nebula.connection.NebulaMetaConnectionProvider;
import org.apache.flink.connector.nebula.statement.ExecutionOptions;
import org.apache.flink.connector.nebula.utils.FailureHandlerEnum;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class NebulaBatchOutputFormat<T, OptionsT extends ExecutionOptions>
        extends RichOutputFormat<T> implements Flushable {
    private static final Logger LOG = LoggerFactory.getLogger(NebulaBatchOutputFormat.class);
    private static final long serialVersionUID = 8846672119763512586L;
    protected MetaClient metaClient;
    protected final NebulaMetaConnectionProvider metaProvider;
    protected final NebulaGraphConnectionProvider graphProvider;
    protected final OptionsT executionOptions;
    protected NebulaBatchExecutor<T> nebulaBatchExecutor;
    private transient long numPendingRow;
    private NebulaPool nebulaPool;
    private Session session;

    private transient ScheduledExecutorService scheduler;
    private transient ScheduledFuture<?> scheduledFuture;
    private transient volatile boolean closed = false;
    private transient volatile Exception commitException;

    public NebulaBatchOutputFormat(
            NebulaGraphConnectionProvider graphProvider,
            NebulaMetaConnectionProvider metaProvider,
            OptionsT executionOptions) {
        this.graphProvider = graphProvider;
        this.metaProvider = metaProvider;
        this.executionOptions = executionOptions;
    }

    @Override
    public void configure(Configuration configuration) {
    }

    /**
     * prepare all resources
     */
    @Override
    public void open(int i, int i1) throws IOException {
        try {
            nebulaPool = graphProvider.getNebulaPool();
        } catch (UnknownHostException e) {
            LOG.error("failed to create connection pool", e);
            throw new IOException("connection pool creation error", e);
        }
        renewSession();

        try {
            metaClient = metaProvider.getMetaClient();
        } catch (TException | ClientServerIncompatibleException e) {
            LOG.error("failed to get meta client", e);
            throw new IOException("get meta client error", e);
        }

        numPendingRow = 0;
        nebulaBatchExecutor = createNebulaBatchExecutor();
        // start the schedule task: submit the buffer records every batchInterval.
        // If batchIntervalMs is 0, do not start the scheduler task.
        if (executionOptions.getBatchIntervalMs() != 0 && executionOptions.getBatchSize() != 1) {
            this.scheduler = Executors.newScheduledThreadPool(1, new ExecutorThreadFactory(
                    "nebula-write-output-format"));
            this.scheduledFuture = this.scheduler.scheduleWithFixedDelay(() -> {
                synchronized (NebulaBatchOutputFormat.this) {
                    if (!closed && commitException == null) {
                        try {
                            commit();
                        } catch (Exception e) {
                            commitException = e;
                        }
                    }
                } },
                    executionOptions.getBatchIntervalMs(),
                    executionOptions.getBatchIntervalMs(),
                    TimeUnit.MILLISECONDS);
        }
    }

    private void checkCommitException() {
        if (commitException != null) {
            throw new RuntimeException("commit records failed", commitException);
        }
    }

    private void renewSession() throws IOException {
        if (session != null) {
            session.release();
            session = null;
        }
        try {
            session = nebulaPool.getSession(graphProvider.getUserName(),
                    graphProvider.getPassword(), true);
        } catch (NotValidConnectionException | AuthFailedException
                 | ClientServerIncompatibleException | IOErrorException e) {
            LOG.error("failed to get graph session", e);
            throw new IOException("get graph session error", e);
        }
        ResultSet resultSet;
        try {
            resultSet = session.execute("USE " + executionOptions.getGraphSpace());
        } catch (IOErrorException e) {
            LOG.error("switch space error", e);
            throw new IOException("switch space error", e);
        }
        if (!resultSet.isSucceeded()) {
            LOG.error("switch space failed: " + resultSet.getErrorMessage());
            throw new IOException("switch space failed: " + resultSet.getErrorMessage());
        }
    }

    protected abstract NebulaBatchExecutor<T> createNebulaBatchExecutor();

    /**
     * write one record to buffer
     */
    @Override
    public final synchronized void writeRecord(T row) throws IOException {
        checkCommitException();
        nebulaBatchExecutor.addToBatch(row);
        numPendingRow++;

        if (executionOptions.getBatchSize() > 0
                && numPendingRow >= executionOptions.getBatchSize()) {
            commit();
        }
    }

    /**
     * commit batch insert statements
     */
    private synchronized void commit() throws IOException {
        int maxRetries = executionOptions.getMaxRetries();
        int retryDelayMs = executionOptions.getRetryDelayMs();
        boolean failOnError = executionOptions.getFailureHandler().equals(FailureHandlerEnum.FAIL);

        // execute the batch at most `maxRetries + 1` times
        for (int i = 0; i <= maxRetries; i++) {
            try {
                nebulaBatchExecutor.executeBatch(session);
                numPendingRow = 0;
                break;
            } catch (Exception e) {
                LOG.error(String.format("write data error (attempt %s)", i), e);
                if (i >= maxRetries) {
                    // clear the batch on failure after all retries
                    nebulaBatchExecutor.clearBatch();
                    numPendingRow = 0;
                    if (failOnError) {
                        throw e;
                    }
                } else if (i + 1 <= maxRetries) {
                    try {
                        Thread.sleep(retryDelayMs);
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        throw new IOException("interrupted", ex);
                    }
                }
                // We do not know whether the failure was due to an expired session or
                // an issue with the query, so we renew the session anyway to be more robust.
                renewSession();
            }
        }
    }

    /**
     * commit the batch write operator before release connection
     */
    @Override
    public final synchronized void close() throws IOException {
        if (!closed) {
            closed = true;
            if (scheduledFuture != null) {
                scheduledFuture.cancel(false);
                scheduler.shutdown();
            }
            if (numPendingRow > 0) {
                commit();
            }
            if (session != null) {
                session.release();
            }
            if (nebulaPool != null) {
                nebulaPool.close();
            }
            if (metaClient != null) {
                metaClient.close();
            }
        }
    }

    /**
     * commit the batch write operator
     */
    @Override
    public synchronized void flush() throws IOException {
        checkCommitException();
        while (numPendingRow > 0) {
            commit();
        }
    }
}
