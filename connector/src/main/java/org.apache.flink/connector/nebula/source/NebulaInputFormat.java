/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.source;

import com.vesoft.nebula.driver.graph.scan.TableRow;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.nebula.connection.GraphProvider;
import org.apache.flink.connector.nebula.options.ConnectionOptions;
import org.apache.flink.connector.nebula.options.ExecutionOptions;
import org.apache.flink.connector.nebula.options.SourceExecutionOptions;
import org.apache.flink.connector.nebula.options.SourceNodeOptions;
import org.apache.flink.connector.nebula.utils.PartitionUtils;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * InputFormat to read data from NebulaGraph and generate Rows.
 * The InputFormat has to be configured using ConnectionOptions and SourceExecutionOptions.
 *
 * @see Row
 * @see ConnectionOptions
 * @see SourceExecutionOptions
 */
public abstract class NebulaInputFormat<T> extends RichInputFormat<T, InputSplit> {
    protected static final Logger LOG = LoggerFactory.getLogger(NebulaInputFormat.class);

    private static final long serialVersionUID = 902031944252613459L;

    protected ConnectionOptions      connectionOptions;
    protected SourceExecutionOptions executionOptions;

    protected Boolean        hasNext = false;
    protected List<TableRow> rows;

    private   NebulaSource       nebulaSource;
    protected NebulaConverter<T> nebulaConverter;

    private long scannedRows;
    /**
     * the number of graph partitions
     */
    private int  numPart;
    private int  times = 0;

    public NebulaInputFormat(ConnectionOptions connectionOptions,
                             SourceExecutionOptions executionOptions) {
        this.connectionOptions = connectionOptions;
        this.executionOptions = executionOptions;
    }

    @Override
    public void configure(Configuration configuration) {
        // do nothing
    }

    @Override
    public void openInputFormat() throws IOException {
        GraphProvider graphProvider = null;
        try {
            graphProvider = new GraphProvider(connectionOptions);
            numPart = graphProvider.getAllParts().size();
        } catch (Exception e) {
            LOG.error("get all partitions error, ", e);
            throw new IOException("get all partitions error, ", e);
        } finally {
            if (graphProvider != null) {
                graphProvider.close();
            }
        }
        rows = new ArrayList<>();
    }

    @Override
    public void closeInputFormat() {
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics baseStatistics) {
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
                                                                  numPart,
                                                                  split.getTotalNumberOfSplits());
            if (executionOptions instanceof SourceNodeOptions) {
                nebulaSource = new NebulaNodeSource(connectionOptions, executionOptions, scanParts);
            } else {
                nebulaSource = new NebulaEdgeSource(connectionOptions, executionOptions, scanParts);
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
        LOG.debug("source nextRecord: {}", times++);

        TableRow row = nebulaSource.next();
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

}
