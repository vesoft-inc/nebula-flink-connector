/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.table;

import org.apache.flink.connector.nebula.connection.NebulaClientOptions;
import org.apache.flink.connector.nebula.connection.NebulaStorageConnectionProvider;
import org.apache.flink.connector.nebula.source.NebulaSourceFunction;
import org.apache.flink.connector.nebula.statement.ExecutionOptions;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

public class NebulaDynamicTableSource implements ScanTableSource, LookupTableSource,
    SupportsProjectionPushDown {
    private final String metaAddress;
    private final String graphAddress;
    private final String username;
    private final String password;

    private final NebulaClientOptions clientOptions;

    private final ExecutionOptions executionOptions;
    private final DataType producedDataType;

    public NebulaDynamicTableSource(
        NebulaClientOptions clientOptions,
        ExecutionOptions executionOptions,
        DataType producedDataType) {
        this.clientOptions = clientOptions;
        this.metaAddress = clientOptions.getRawMetaAddress();
        this.graphAddress = clientOptions.getGraphAddress();
        this.username = clientOptions.getUsername();
        this.password = clientOptions.getPassword();
        this.executionOptions = executionOptions;
        this.producedDataType = producedDataType;
    }

    @Override
    public DynamicTableSource copy() {
        return new NebulaDynamicTableSource(clientOptions, executionOptions, producedDataType);
    }

    @Override
    public String asSummaryString() {
        return "Nebula";
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {

        return null;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        NebulaClientOptions builder =
            new NebulaClientOptions.NebulaClientOptionsBuilder()
                .setMetaAddress(metaAddress)
                .setGraphAddress(graphAddress)
                .setUsername(username)
                .setPassword(password)
                .build();

        NebulaStorageConnectionProvider storageConnectionProvider =
            new NebulaStorageConnectionProvider(builder);

        NebulaSourceFunction<RowData> sourceFunction =
            new NebulaSourceFunction<>(storageConnectionProvider);

        sourceFunction.setExecutionOptions(executionOptions);
        DataStructureConverter converter =
            runtimeProviderContext.createDataStructureConverter(producedDataType);
        sourceFunction.setConverter(converter);
        return SourceFunctionProvider.of(sourceFunction, true);
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields) {

    }
}
