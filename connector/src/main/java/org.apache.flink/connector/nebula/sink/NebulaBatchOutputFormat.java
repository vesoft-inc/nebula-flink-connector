/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.sink;

import java.io.Flushable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.nebula.connection.GraphProvider;
import org.apache.flink.connector.nebula.options.ConnectionOptions;
import org.apache.flink.connector.nebula.options.ExecutionOptions;
import org.apache.flink.types.Row;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class NebulaBatchOutputFormat<T, OptionsT extends ExecutionOptions>
        extends RichOutputFormat<T> implements Flushable {
    private static final Logger LOG = LoggerFactory.getLogger(NebulaBatchOutputFormat.class);

    private static final long                   serialVersionUID = 8846672119763512586L;
    protected            GraphProvider          graphProvider;
    protected final      OptionsT               executionOptions;
    protected final      ConnectionOptions      connectionOptions;
    protected            NebulaBatchExecutor<T> nebulaBatchExecutor;
    private volatile     AtomicLong             numPendingRow;
    private final        List<String>           errorBuffer      = new ArrayList<>();

    private transient          ScheduledExecutorService scheduler;
    private transient          ScheduledFuture<?>       scheduledFuture;
    private transient volatile boolean                  closed = false;

    public NebulaBatchOutputFormat(ConnectionOptions connectionOptions, OptionsT executionOptions) {
        this.connectionOptions = connectionOptions;
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
            graphProvider = new GraphProvider(connectionOptions);
        } catch (Exception e) {
            LOG.error("failed to get NebulaPool, ", e);
            throw new IOException("get NebulaPool error, ", e);
        }

        numPendingRow = new AtomicLong(0);
        nebulaBatchExecutor = createNebulaBatchExecutor();
        // start the schedule task: submit the buffer records every batchInterval.
        // If batchIntervalMs is 0, do not start the scheduler task.
        if (executionOptions.getIntervalMs() != 0 && executionOptions.getBatchSize() != 1) {
            this.scheduler = Executors.newScheduledThreadPool(1, new ExecutorThreadFactory(
                    "nebula-write-output-format"));
            Runnable thread = () -> {
                synchronized (NebulaBatchOutputFormat.this) {
                    if (!closed) {
                        commit();
                    }
                }
            };
            this.scheduledFuture = this.scheduler
                    .scheduleWithFixedDelay(thread,
                                            executionOptions.getIntervalMs(),
                                            executionOptions.getIntervalMs(),
                                            TimeUnit.MILLISECONDS);
        }
    }

    protected abstract NebulaBatchExecutor<T> createNebulaBatchExecutor();

    /**
     * write one record to buffer
     */
    @Override
    public final synchronized void writeRecord(T row) {
        LOG.debug(">>>>> write row: {}", row.toString());
        nebulaBatchExecutor.addToBatch(row);

        if (numPendingRow.incrementAndGet() >= executionOptions.getBatchSize()) {
            LOG.debug(">>>>> numPendingRow size:{}, now commit the batch rows.",
                      numPendingRow.get());
            commit();
        }
    }

    /**
     * commit batch insert statements
     */
    private synchronized void commit() {
        String errorExec = nebulaBatchExecutor.executeBatch(graphProvider);
        if (errorExec != null) {
            errorBuffer.add(errorExec);
            LOG.debug(">>>>> error: {}", errorExec);
        }
        long pendingRow = numPendingRow.get();
        numPendingRow.compareAndSet(pendingRow, 0);
        LOG.debug(">>>>> set pendingRow size to {}.", numPendingRow.get());
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
            if (!errorBuffer.isEmpty()) {
                LOG.error("insert failed statements size: {}", errorBuffer.size());
            }

            if (graphProvider != null) {
                graphProvider.close();
            }
        }
    }

    /**
     * commit the batch write operator
     */
    @Override
    public synchronized void flush() {
        while (numPendingRow.get() != 0) {
            commit();
        }
    }
}
