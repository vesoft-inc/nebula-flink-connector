/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.source;

import com.vesoft.nebula.client.meta.MetaClient;
import com.vesoft.nebula.client.storage.StorageClient;
import com.vesoft.nebula.client.storage.data.BaseTableRow;
import java.util.List;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.nebula.connection.NebulaClientOptions;
import org.apache.flink.connector.nebula.connection.NebulaMetaConnectionProvider;
import org.apache.flink.connector.nebula.connection.NebulaStorageConnectionProvider;
import org.apache.flink.connector.nebula.statement.ExecutionOptions;
import org.apache.flink.connector.nebula.utils.PartitionUtils;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link RichParallelSourceFunction} to get NebulaGraph vertex and edge.
 */
public class NebulaSourceFunction<T> extends RichParallelSourceFunction<T> {

    private static final Logger LOG = LoggerFactory.getLogger(NebulaSourceFunction.class);

    private static final long serialVersionUID = -4864517634021753949L;

    private StorageClient storageClient;
    private MetaClient metaClient;
    private final NebulaStorageConnectionProvider storageConnectionProvider;
    private final NebulaMetaConnectionProvider metaConnectionProvider;
    private ExecutionOptions executionOptions;
    private final NebulaRowConverter nebulaRowConverter;
    private DynamicTableSource.DataStructureConverter converter;
    /**
     * the number of graph partitions
     */
    private int numPart;

    public NebulaSourceFunction(NebulaStorageConnectionProvider storageConnectionProvider) {
        super();
        this.nebulaRowConverter = new NebulaRowConverter();
        this.storageConnectionProvider = storageConnectionProvider;
        NebulaClientOptions nebulaClientOptions =
                storageConnectionProvider.getNebulaClientOptions();
        this.metaConnectionProvider =
                new NebulaMetaConnectionProvider(nebulaClientOptions);
    }

    /**
     * open nebula client
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        storageClient = storageConnectionProvider.getStorageClient();
        metaClient = metaConnectionProvider.getMetaClient();
        numPart = metaClient.getPartsAlloc(executionOptions.getGraphSpace()).size();
    }

    /**
     * close nebula client
     */
    @Override
    public void close() throws Exception {
        try {
            if (storageClient != null) {
                storageClient.close();
            }
            if (metaClient != null) {
                metaClient.close();
            }
        } catch (Exception e) {
            LOG.error("cancel exception:{}", e.getMessage(), e);
        }
    }

    /**
     * execute scan nebula data
     */
    @Override
    public void run(SourceContext<T> sourceContext) throws Exception {
        RuntimeContext runtimeContext = getRuntimeContext();
        List<Integer> scanParts = PartitionUtils.getScanParts(
                runtimeContext.getIndexOfThisSubtask() + 1,
                numPart,
                runtimeContext.getNumberOfParallelSubtasks());

        NebulaSource<BaseTableRow> nebulaSource;
        if (executionOptions.getDataType().isVertex()) {
            nebulaSource = new NebulaVertexSource(storageClient, executionOptions, scanParts);
        } else {
            nebulaSource = new NebulaEdgeSource(storageClient, executionOptions, scanParts);
        }

        while (nebulaSource.hasNext()) {
            BaseTableRow baseTableRow = nebulaSource.next();
            Row row = nebulaRowConverter.convert(baseTableRow);
            sourceContext.collect((T) converter.toInternal(row));
        }
    }

    @Override
    public void cancel() {
        try {
            if (storageClient != null) {
                storageClient.close();
            }
            if (metaClient != null) {
                metaClient.close();
            }
        } catch (Exception e) {
            LOG.error("cancel exception:{}", e.getMessage(), e);
        }
    }

    public NebulaSourceFunction<T> setExecutionOptions(ExecutionOptions executionOptions) {
        this.executionOptions = executionOptions;
        return this;
    }

    public NebulaSourceFunction<T> setConverter(
        DynamicTableSource.DataStructureConverter converter) {
        this.converter = converter;
        return this;
    }
}
