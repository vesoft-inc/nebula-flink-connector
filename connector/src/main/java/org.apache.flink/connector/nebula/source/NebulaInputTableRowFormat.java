/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.source;

import com.vesoft.nebula.client.storage.data.BaseTableRow;
import java.io.IOException;
import org.apache.flink.connector.nebula.connection.NebulaMetaConnectionProvider;
import org.apache.flink.connector.nebula.connection.NebulaStorageConnectionProvider;
import org.apache.flink.connector.nebula.statement.ExecutionOptions;
import org.apache.flink.core.io.InputSplit;

/**
 * implementation of NebulaInputFormat.
 * Read NebulaGraph data in nebula's {@link BaseTableRow} format.
 * <b>how to use:
 *   NebulaInputTableRowFormat inputFormat = new NebulaInputTableRowFormat
 *                                          (storageConnectionProvider,
 *                                          metaConnectionProvider, vertexExecutionOptions);
 *   DataSource dataSource = env.createInput(inputFormat);
 * </b>
 */
public class NebulaInputTableRowFormat extends NebulaInputFormat<BaseTableRow> {

    public NebulaInputTableRowFormat(NebulaStorageConnectionProvider storageConnectionProvider,
                                     NebulaMetaConnectionProvider metaConnectionProvider,
                                     ExecutionOptions executionOptions) {
        super(storageConnectionProvider, metaConnectionProvider, executionOptions);
    }

    @Override
    public void open(InputSplit inputSplit) throws IOException {
        super.open(inputSplit);
        super.nebulaConverter = new NebulaBaseTableRowConverter();
    }
}
