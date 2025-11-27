/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.source;

import com.vesoft.nebula.driver.graph.scan.TableRow;
import java.io.IOException;
import org.apache.flink.connector.nebula.options.ConnectionOptions;
import org.apache.flink.connector.nebula.options.SourceExecutionOptions;
import org.apache.flink.core.io.InputSplit;

/**
 * implementation of NebulaInputFormat.
 * Read NebulaGraph data in nebula's {@link TableRow} format.
 * <b>how to use:
 * NebulaInputTableRowFormat inputFormat = new NebulaInputTableRowFormat (connectionOptions,
 *                                                                        executionOptions);
 * DataSource dataSource = env.createInput(inputFormat);
 * </b>
 */
public class NebulaInputTableRowFormat extends NebulaInputFormat<TableRow> {

    public NebulaInputTableRowFormat(ConnectionOptions connectionOptions,
                                     SourceExecutionOptions executionOptions) {
        super(connectionOptions, executionOptions);
    }

    @Override
    public void open(InputSplit inputSplit) throws IOException {
        super.open(inputSplit);
        super.nebulaConverter = new NebulaBaseTableRowConverter();
    }
}
