/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package org.apache.flink.connector.nebula.source;

import com.vesoft.nebula.client.storage.StorageClient;
import com.vesoft.nebula.client.storage.data.BaseTableRow;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.nebula.connection.NebulaStorageConnectionProvider;
import org.apache.flink.connector.nebula.statement.ExecutionOptions;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link RichSourceFunction} to get NebulaGraph vertex and edge.
 */
public class NebulaSourceFunction extends RichSourceFunction<BaseTableRow> {

    private static final Logger LOG = LoggerFactory.getLogger(NebulaSourceFunction.class);

    private static final long serialVersionUID = -4864517634021753949L;

    private StorageClient storageClient;
    private final NebulaStorageConnectionProvider connectionProvider;
    private ExecutionOptions executionOptions;

    public NebulaSourceFunction(NebulaStorageConnectionProvider connectionProvider) {
        super();
        this.connectionProvider = connectionProvider;
    }


    /**
     * open nebula client
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        storageClient = connectionProvider.getStorageClient();
    }

    /**
     * close nebula client
     */
    @Override
    public void close() throws Exception {
        if (storageClient != null) {
            storageClient.close();
        }
    }

    /**
     * execute scan nebula data
     */
    @Override
    public void run(SourceContext<BaseTableRow> sourceContext) throws Exception {

        Map<String, List<String>> returnCols = new HashMap<>();
        returnCols.put(executionOptions.getLabel(), executionOptions.getFields());

        NebulaSource nebulaSource;
        if (executionOptions.getDataType().isVertex()) {
            nebulaSource = new NebulaVertexSource(storageClient, executionOptions);
        } else {
            nebulaSource = new NebulaEdgeSource(storageClient, executionOptions);
        }

        while (nebulaSource.hasNext()) {
            BaseTableRow row = nebulaSource.next();
            sourceContext.collect(row);
        }
    }

    @Override
    public void cancel() {
        try {
            if (storageClient != null) {
                storageClient.close();
            }
        } catch (Exception e) {
            LOG.error("cancel exception:{}", e);
        }
    }

    public NebulaSourceFunction setExecutionOptions(ExecutionOptions executionOptions) {
        this.executionOptions = executionOptions;
        return this;
    }
}
