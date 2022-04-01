/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.source;

import com.vesoft.nebula.client.meta.MetaClient;
import com.vesoft.nebula.client.storage.StorageClient;
import com.vesoft.nebula.client.storage.data.BaseTableRow;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.nebula.connection.NebulaMetaConnectionProvider;
import org.apache.flink.connector.nebula.connection.NebulaStorageConnectionProvider;
import org.apache.flink.connector.nebula.statement.ExecutionOptions;
import org.apache.flink.connector.nebula.utils.PartitionUtils;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * InputFormat to read data from NebulaGraph and generate Rows.
 * The InputFormat has to be configured using the supplied
 * NebulaStorageConnectionProvider and ExecutionOptions.
 *
 * @see Row
 * @see NebulaStorageConnectionProvider
 * @see ExecutionOptions
 */
abstract class NebulaInputFormat<T> extends RichInputFormat<T, InputSplit> {
    protected static final Logger LOG = LoggerFactory.getLogger(NebulaInputFormat.class);
    private static final long serialVersionUID = 902031944252613459L;

    protected ExecutionOptions executionOptions;
    protected NebulaStorageConnectionProvider storageConnectionProvider;
    protected NebulaMetaConnectionProvider metaConnectionProvider;
    private transient StorageClient storageClient;
    private transient MetaClient metaClient;

    protected Boolean hasNext = false;
    protected List<BaseTableRow> rows;

    private NebulaSource nebulaSource;
    protected NebulaConverter<T> nebulaConverter;

    private long scannedRows;
    /**
     * the number of graph partitions
     */
    private Integer numPart;
    private int times = 0; // todo rm

    public NebulaInputFormat(NebulaStorageConnectionProvider storageConnectionProvider,
                             NebulaMetaConnectionProvider metaConnectionProvider,
                             ExecutionOptions executionOptions) {
        this.storageConnectionProvider = storageConnectionProvider;
        this.metaConnectionProvider = metaConnectionProvider;
        this.executionOptions = executionOptions;
    }

    @Override
    public void configure(Configuration configuration) {
        // do nothing
    }

    @Override
    public void openInputFormat() throws IOException {
        try {
            storageClient = storageConnectionProvider.getStorageClient();
            metaClient = metaConnectionProvider.getMetaClient();
            numPart = metaClient.getPartsAlloc(executionOptions.getGraphSpace()).size();
        } catch (Exception e) {
            LOG.error("connect storage client error, ", e);
            throw new IOException("connect storage client error, ", e);
        }
        rows = new ArrayList<>();
    }

    @Override
    public void closeInputFormat() throws IOException {
        try {
            if (storageClient != null) {
                storageClient.close();
            }
            if (metaClient != null) {
                metaClient.close();
            }
        } catch (Exception e) {
            LOG.error("close client error,", e);
            throw new IOException("close client error,", e);
        }
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics baseStatistics) throws IOException {
        return baseStatistics;
    }

    @Override
    public InputSplit[] createInputSplits(int numSplit) throws IOException {
        InputSplit[] inputSplits = new InputSplit[numSplit];
        for (int i = 0; i < numSplit; i++) {
            inputSplits[i] = new GenericInputSplit(i + 1, numSplit);
        }
        return inputSplits;
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
        return new DefaultInputSplitAssigner(inputSplits);
    }

    @Override
    public void open(InputSplit inputSplit) throws IOException {
        if (inputSplit != null) {
            GenericInputSplit split = (GenericInputSplit) inputSplit;
            List<Integer> scanParts = PartitionUtils.getScanParts(split.getSplitNumber(),
                    numPart, split.getTotalNumberOfSplits());
            if (executionOptions.getDataType().isVertex()) {
                nebulaSource = new NebulaVertexSource(storageClient, executionOptions, scanParts);
            } else {
                nebulaSource = new NebulaEdgeSource(storageClient, executionOptions, scanParts);
            }
            try {
                hasNext = nebulaSource.hasNext();
            } catch (Exception e) {
                LOG.error("scan NebulaGraph error, ", e);
                throw new IOException("scan error, ", e);
            }
        }
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return !hasNext;
    }

    @Override
    public T nextRecord(T reuse) throws IOException {
        if (!hasNext) {
            return null;
        }
        LOG.info("nextRecord: {}", times++);

        BaseTableRow row = nebulaSource.next();
        try {
            hasNext = nebulaSource.hasNext();
        } catch (Exception e) {
            LOG.error("scan NebulaGraph error, ", e);
            throw new IOException("scan NebulaGraph error, ", e);
        }
        scannedRows++;
        return nebulaConverter.convert(row);
    }

    @Override
    public void close() {
        LOG.info("Closing split (scanned {} rows)", scannedRows);
    }

    public NebulaInputFormat setExecutionOptions(ExecutionOptions executionOptions) {
        this.executionOptions = executionOptions;
        return this;
    }

}
