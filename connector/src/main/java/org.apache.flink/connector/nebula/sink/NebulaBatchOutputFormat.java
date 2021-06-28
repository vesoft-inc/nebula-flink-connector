/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package org.apache.flink.connector.nebula.sink;

import com.facebook.thrift.TException;
import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.exception.AuthFailedException;
import com.vesoft.nebula.client.graph.exception.IOErrorException;
import com.vesoft.nebula.client.graph.exception.NotValidConnectionException;
import com.vesoft.nebula.client.graph.net.Session;
import com.vesoft.nebula.client.meta.MetaClient;
import java.io.Flushable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
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
    private Class<? extends NebulaOutputFormatConverter<T>> converterClazz = null;

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
        } catch (NotValidConnectionException | IOErrorException | AuthFailedException e) {
            LOG.error("failed to get graph session, ", e);
            throw new IOException("get graph session error, ", e);
        }
        try {
            metaClient = metaProvider.getMetaClient();
        } catch (TException e) {
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
        } else {
            schema = metaProvider.getEdgeSchema(metaClient, executionOptions.getGraphSpace(),
                    executionOptions.getLabel());
        }

        try{
            nebulaBatchExecutor = new NebulaBatchExecutor(executionOptions, isVertex, vidType, schema, converterClazz);
        } catch (NoSuchMethodException | SecurityException e){
            LOG.error("failed to get NebulaOutputFormatConverter constructor: ParameterType: (ExecutionOptions, VidTypeEnum, Map<String, Integer>)", e);
            throw new IOException("get NebulaOutputFormatConverter constructor error", e);
        } catch (IllegalAccessException | InvocationTargetException | IllegalArgumentException | InstantiationException e) {
            LOG.error("failed to get NebulaOutputFormatConverter instance", e);
            throw new IOException("get NebulaOutputFormatConverter instance error", e);
        } catch (Exception e) {
            LOG.error("failed to provide a legitimate NebulaOutputFormatConverter implementation", e);
            throw new IOException("get NebulaOutputFormatConverter implementation error", e);
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
        ResultSet resultSet;
        try {
            resultSet = session.execute("USE " + executionOptions.getGraphSpace());
        } catch (IOErrorException e) {
            LOG.error("switch space error, ", e);
            throw new IOException("switch space error,", e);
        }

        if (resultSet.isSucceeded()) {
            String errorExec = nebulaBatchExecutor.executeBatch(session);
            if (errorExec != null) {
                errorBuffer.add(errorExec);
            }
            long pendingRow = numPendingRow.get();
            numPendingRow.compareAndSet(pendingRow, 0);
        } else {
            LOG.error("switch space failed, ", resultSet.getErrorMessage());
        }
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
            LOG.error("insert error statements: ", errorBuffer);
        }
        session.release();
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

    /**
     * customize your own NebulaOutputFormatConverter implementation that converts data into queries
     *
     * @param converterClazz
     * @return
     */
    public NebulaBatchOutputFormat<T> setConverterClazz(Class<? extends NebulaOutputFormatConverter<T>> converterClazz) {
        this.converterClazz = converterClazz;
        return this;
    }
}
