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
import java.util.ArrayList;
import java.util.List;
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
    private volatile AtomicLong numPendingRow;
    private NebulaPool nebulaPool;
    private Session session;

    private transient ScheduledExecutorService scheduler;
    private transient ScheduledFuture<?> scheduledFuture;
    private transient volatile boolean closed = false;

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
            session = nebulaPool.getSession(graphProvider.getUserName(),
                    graphProvider.getPassword(), true);
        } catch (UnknownHostException | NotValidConnectionException | AuthFailedException
                | ClientServerIncompatibleException | IOErrorException e) {
            LOG.error("failed to get graph session, ", e);
            throw new IOException("get graph session error, ", e);
        }
        ResultSet resultSet;
        try {
            resultSet = session.execute("USE " + executionOptions.getGraphSpace());
        } catch (IOErrorException e) {
            LOG.error("switch space error, ", e);
            throw new IOException("switch space error,", e);
        }
        if (!resultSet.isSucceeded()) {
            LOG.error("switch space failed, {}", resultSet.getErrorMessage());
            throw new RuntimeException("switch space failed, " + resultSet.getErrorMessage());
        }

        try {
            metaClient = metaProvider.getMetaClient();
        } catch (TException | ClientServerIncompatibleException e) {
            LOG.error("failed to get meta client, ", e);
            throw new IOException("get metaClient error, ", e);
        }

        numPendingRow = new AtomicLong(0);
        nebulaBatchExecutor = createNebulaBatchExecutor();
        // start the schedule task: submit the buffer records every batchInterval.
        // If batchIntervalMs is 0, do not start the scheduler task.
        if (executionOptions.getBatchIntervalMs() != 0 && executionOptions.getBatchSize() != 1) {
            this.scheduler = Executors.newScheduledThreadPool(1, new ExecutorThreadFactory(
                    "nebula-write-output-format"));
            this.scheduledFuture = this.scheduler.scheduleWithFixedDelay(() -> {
                synchronized (NebulaBatchOutputFormat.this) {
                    if (!closed) {
                        commit();
                    }
                } },
                    executionOptions.getBatchIntervalMs(),
                    executionOptions.getBatchIntervalMs(),
                    TimeUnit.MILLISECONDS);
        }
    }

    protected abstract NebulaBatchExecutor<T> createNebulaBatchExecutor();

    /**
     * write one record to buffer
     */
    @Override
    public final synchronized void writeRecord(T row) {
        nebulaBatchExecutor.addToBatch(row);

        if (numPendingRow.incrementAndGet() >= executionOptions.getBatchSize()) {
            commit();
        }
    }

    /**
     * commit batch insert statements
     */
    private synchronized void commit() {
        nebulaBatchExecutor.executeBatch(session);
        long pendingRow = numPendingRow.get();
        numPendingRow.compareAndSet(pendingRow, 0);
    }

    /**
     * commit the batch write operator before release connection
     */
    @Override
    public final synchronized void close() {
        if (!closed) {
            closed = true;
            if (scheduledFuture != null) {
                scheduledFuture.cancel(false);
                scheduler.shutdown();
            }
            if (numPendingRow != null && numPendingRow.get() > 0) {
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
        while (numPendingRow.get() != 0) {
            commit();
        }
    }
}
