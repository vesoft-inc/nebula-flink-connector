/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.source;

import com.vesoft.nebula.driver.graph.scan.TableRow;
import org.apache.flink.connector.nebula.connection.GraphProvider;
import org.apache.flink.connector.nebula.options.ConnectionOptions;
import org.apache.flink.connector.nebula.options.SourceExecutionOptions;

/**
 * NebulaSource is the reader to read NebulaGraph's data iteratively.
 */
abstract class NebulaSource {

    protected ConnectionOptions      connectionOptions;
    protected SourceExecutionOptions executionOptions;
    protected GraphProvider          graphProvider;

    public NebulaSource(ConnectionOptions connectionOptions,
                        SourceExecutionOptions executionOptions) {
        this.connectionOptions = connectionOptions;
        this.executionOptions = executionOptions;
        this.graphProvider = new GraphProvider(connectionOptions);
    }

    /**
     * if source has more data
     */
    abstract boolean hasNext() throws Exception;

    /**
     * get another Nebula Graph data
     */
    abstract TableRow next();
}
