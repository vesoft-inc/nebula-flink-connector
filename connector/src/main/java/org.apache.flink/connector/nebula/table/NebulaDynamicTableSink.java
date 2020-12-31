package org.apache.flink.connector.nebula.table;

import org.apache.flink.connector.nebula.connection.NebulaClientOptions;
import org.apache.flink.connector.nebula.connection.NebulaGraphConnectionProvider;
import org.apache.flink.connector.nebula.connection.NebulaMetaConnectionProvider;
import org.apache.flink.connector.nebula.sink.NebulaBatchOutputFormat;
import org.apache.flink.connector.nebula.sink.NebulaSinkFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

public class NebulaDynamicTableSink implements DynamicTableSink {
    private final String metaAddress;
    private final String graphAddress;


    private final String username;
    private final String password;

    public NebulaDynamicTableSink(String metaAddress, String graphAddress, String username,
                                  String password) {
        this.metaAddress = metaAddress;
        this.graphAddress = graphAddress;
        this.username = username;
        this.password = password;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        ChangelogMode.Builder builder = ChangelogMode.newBuilder();
        for (RowKind kind : requestedMode.getContainedKinds()) {
            if (kind != RowKind.UPDATE_BEFORE) {
                builder.addContainedKind(kind);
            }
        }
        return builder.build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {

        NebulaClientOptions builder = new NebulaClientOptions.NebulaClientOptionsBuilder()
                .setMetaAddress(metaAddress)
                .setGraphAddress(graphAddress)
                .setUsername(username)
                .setPassword(password)
                .build();

        NebulaGraphConnectionProvider graphProvider = new NebulaGraphConnectionProvider(builder);
        NebulaMetaConnectionProvider metaProvider = new NebulaMetaConnectionProvider(builder);
        NebulaBatchOutputFormat<RowData> outPutFormat = new NebulaBatchOutputFormat(graphProvider,
                metaProvider);
        NebulaSinkFunction<RowData> sinkFunction = new NebulaSinkFunction<>(outPutFormat);
        return SinkFunctionProvider.of(sinkFunction);
    }

    @Override
    public DynamicTableSink copy() {
        return null;
    }

    @Override
    public String asSummaryString() {
        return null;
    }
}
