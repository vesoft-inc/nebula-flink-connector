/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package org.apache.flink.connector.nebula.sink;

import com.facebook.thrift.TException;
import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.exception.AuthFailedException;
import com.vesoft.nebula.client.graph.exception.ClientServerIncompatibleException;
import com.vesoft.nebula.client.graph.exception.IOErrorException;
import com.vesoft.nebula.client.graph.exception.NotValidConnectionException;
import com.vesoft.nebula.client.graph.net.Session;
import com.vesoft.nebula.client.meta.MetaClient;
import java.io.Flushable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.nebula.connection.NebulaGraphConnectionProvider;
import org.apache.flink.connector.nebula.connection.NebulaMetaConnectionProvider;
import org.apache.flink.connector.nebula.statement.ExecutionOptions;
import org.apache.flink.connector.nebula.utils.VidTypeEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NebulaBatchOutputFormat<T> extends RichOutputFormat<T> implements Flushable {
    private static final Logger LOG = LoggerFactory.getLogger(NebulaBatchOutputFormat.class);
    private static final long serialVersionUID = 8846672119763512586L;

    private volatile AtomicLong numPendingRow;
    private Session session;
    private MetaClient metaClient;
    private NebulaBatchExecutor nebulaBatchExecutor;
    private NebulaGraphConnectionProvider graphProvider;
    private NebulaMetaConnectionProvider metaProvider;
    private ExecutionOptions executionOptions;
    private List<String> errorBuffer = new ArrayList<>();

    public NebulaBatchOutputFormat(NebulaGraphConnectionProvider graphProvider,
                                   NebulaMetaConnectionProvider metaProvider) {
        this.graphProvider = graphProvider;
        this.metaProvider = metaProvider;
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
            session = graphProvider.getSession();
        } catch (NotValidConnectionException | IOErrorException
                | AuthFailedException | ClientServerIncompatibleException e) {
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

        VidTypeEnum vidType = metaProvider.getVidType(metaClient, executionOptions.getGraphSpace());
        boolean isVertex = executionOptions.getDataType().isVertex();
        Map<String, Integer> schema;
        if (isVertex) {
            schema = metaProvider.getTagSchema(metaClient, executionOptions.getGraphSpace(),
                    executionOptions.getLabel());
            nebulaBatchExecutor = new NebulaVertexBatchExecutor(executionOptions, vidType, schema);
        } else {
            schema = metaProvider.getEdgeSchema(metaClient, executionOptions.getGraphSpace(),
                    executionOptions.getLabel());
            nebulaBatchExecutor = new NebulaEdgeBatchExecutor(executionOptions, vidType, schema);
        }
    }

    /**
     * write one record to buffer
     */
    @Override
    public final synchronized void writeRecord(T row) throws IOException {
        nebulaBatchExecutor.addToBatch(row);

        if (numPendingRow.incrementAndGet() >= executionOptions.getBatch()) {
            commit();
        }
    }

    /**
     * commit batch insert statements
     */
    private synchronized void commit() throws IOException {
        String errorExec = nebulaBatchExecutor.executeBatch(session);
        if (errorExec != null) {
            errorBuffer.add(errorExec);
        }
        long pendingRow = numPendingRow.get();
        numPendingRow.compareAndSet(pendingRow, 0);
    }

    /**
     * commit the batch write operator before release connection
     */
    @Override
    public final synchronized void close() throws IOException {
        if (numPendingRow.get() > 0) {
            commit();
        }
        if (!errorBuffer.isEmpty()) {
            LOG.error("insert error statements: {}", errorBuffer);
        }
        if (session != null) {
            session.release();
        }
        if (graphProvider != null) {
            graphProvider.close();
        }
        if (metaClient != null) {
            metaClient.close();
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

    public NebulaBatchOutputFormat<T> setExecutionOptions(ExecutionOptions executionOptions) {
        this.executionOptions = executionOptions;
        return this;
    }
}
