/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.source;

import com.vesoft.nebula.driver.graph.scan.TableRow;
import java.util.List;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.nebula.connection.GraphProvider;
import org.apache.flink.connector.nebula.options.ConnectionOptions;
import org.apache.flink.connector.nebula.options.SourceExecutionOptions;
import org.apache.flink.connector.nebula.options.SourceNodeOptions;
import org.apache.flink.connector.nebula.utils.PartitionUtils;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link RichParallelSourceFunction} to get NebulaGraph vertex and edge.
 */
public class NebulaSourceFunction extends RichParallelSourceFunction<TableRow> {

    private static final Logger LOG = LoggerFactory.getLogger(NebulaSourceFunction.class);

    private static final long serialVersionUID = -4864517634021753949L;

    private SourceExecutionOptions executionOptions;
    private ConnectionOptions      connectionOptions;
    /**
     * the number of graph partitions
     */
    private int                    numPart;

    public NebulaSourceFunction(ConnectionOptions connectionOptions,
                                SourceExecutionOptions sourceExecutionOptions) {
        super();
        this.executionOptions = sourceExecutionOptions;
        this.connectionOptions = connectionOptions;
    }

    /**
     * open nebula client
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        GraphProvider graphProvider = null;
        try {
            graphProvider = new GraphProvider(connectionOptions);
            numPart = graphProvider.getAllParts().size();
        } finally {
            if (graphProvider != null) {
                graphProvider.close();
            }
        }

    }

    /**
     * close nebula client
     */
    @Override
    public void close() {
    }

    /**
     * execute scan nebula data
     */
    @Override
    public void run(SourceContext<TableRow> sourceContext) throws Exception {
        RuntimeContext runtimeContext = getRuntimeContext();
        List<Integer> scanParts = PartitionUtils.getScanParts(
                runtimeContext.getIndexOfThisSubtask() + 1,
                numPart,
                runtimeContext.getNumberOfParallelSubtasks());

        NebulaSource nebulaSource;
        if (executionOptions instanceof SourceNodeOptions) {
            nebulaSource = new NebulaNodeSource(connectionOptions, executionOptions, scanParts);
        } else {
            nebulaSource = new NebulaEdgeSource(connectionOptions, executionOptions, scanParts);
        }

        while (nebulaSource.hasNext()) {
            TableRow row = nebulaSource.next();
            sourceContext.collect(row);
        }
    }

    @Override
    public void cancel() {
    }
}
