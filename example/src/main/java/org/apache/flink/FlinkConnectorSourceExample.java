/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package org.apache.flink;

import com.vesoft.nebula.client.graph.data.ValueWrapper;
import com.vesoft.nebula.client.storage.data.BaseTableRow;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.connector.nebula.connection.NebulaClientOptions;
import org.apache.flink.connector.nebula.connection.NebulaStorageConnectionProvider;
import org.apache.flink.connector.nebula.source.NebulaInputRowFormat;
import org.apache.flink.connector.nebula.source.NebulaInputTableRowFormat;
import org.apache.flink.connector.nebula.source.NebulaSourceFunction;
import org.apache.flink.connector.nebula.statement.EdgeExecutionOptions;
import org.apache.flink.connector.nebula.statement.ExecutionOptions;
import org.apache.flink.connector.nebula.statement.VertexExecutionOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkConnectorSourceExample {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkConnectorSourceExample.class);

    private static NebulaStorageConnectionProvider storageConnectionProvider;
    private static ExecutionOptions vertexExecutionOptions;
    private static ExecutionOptions edgeExecutionOptions;

    /**
     * Read streaming Nebula data only supports BaseTableRow format.
     * Read batch Nebula data supports nebula's BaseTableROw format and flink's Row format.
     */
    public static void main(String[] args) throws Exception {
        initConfig();

        nebulaVertexStreamSource();
        nebulaVertexBatchSource();

        nebulaEdgeStreamSource();
        nebulaEdgeBatchSource();

        System.exit(0);
    }

    public static void initConfig() {
        NebulaClientOptions nebulaClientOptions =
                new NebulaClientOptions.NebulaClientOptionsBuilder()
                        .setMetaAddress("127.0.0.1:9559")
                        .build();
        storageConnectionProvider =
                new NebulaStorageConnectionProvider(nebulaClientOptions);

        // read no property
        vertexExecutionOptions = new VertexExecutionOptions.ExecutionOptionBuilder()
                .setGraphSpace("flinkSource")
                .setTag("player")
                .setNoColumn(false)
                .setFields(Arrays.asList())
                .setLimit(100)
                .builder();

        // read specific properties
        // if you want to read all properties, config: setFields(Arrays.asList())
        edgeExecutionOptions = new EdgeExecutionOptions.ExecutionOptionBuilder()
                .setGraphSpace("flinkSource")
                .setEdge("friend")
                //.setFields(Arrays.asList("src", "dst","degree", "start"))
                .setFields(Arrays.asList())
                .setLimit(100)
                .builder();

    }

    /**
     * read Nebula Graph vertex as stream data source
     */
    public static void nebulaVertexStreamSource() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        NebulaSourceFunction sourceFunction = new NebulaSourceFunction(storageConnectionProvider)
                .setExecutionOptions(vertexExecutionOptions);
        DataStreamSource<BaseTableRow> dataStreamSource = env.addSource(sourceFunction);

        dataStreamSource.map(row -> {
            List<ValueWrapper> values = row.getValues();
            Row record = new Row(1);
            record.setField(0, values.get(0).getValue().getFieldValue());
            return record;
        });
        dataStreamSource.print();
        env.execute("NebulaStreamSource");
    }


    /**
     * read Nebula Graph edge as stream data source
     */
    public static void nebulaEdgeStreamSource() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        // get Nebula Graph data in BaseTableRow format
        NebulaSourceFunction sourceFunction = new NebulaSourceFunction(storageConnectionProvider)
                .setExecutionOptions(edgeExecutionOptions);
        DataStreamSource<BaseTableRow> dataStreamSource = env.addSource(sourceFunction);

        dataStreamSource.map(row -> {
            List<ValueWrapper> values = row.getValues();
            Row record = new Row(6);
            record.setField(0, values.get(0).getValue().getFieldValue());
            record.setField(1, values.get(1).getValue().getFieldValue());
            record.setField(2, values.get(2).getValue().getFieldValue());
            record.setField(3, values.get(3).getValue().getFieldValue());
            record.setField(4, values.get(4).getValue().getFieldValue());
            record.setField(5, values.get(5).getValue().getFieldValue());
            return record;
        });
        dataStreamSource.print();
        env.execute("NebulaStreamSource");
    }

    /**
     * read Nebula Graph vertex as batch data source
     */
    public static void nebulaVertexBatchSource() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // get Nebula vertex data in flink Row format
        NebulaInputRowFormat inputRowFormat = new NebulaInputRowFormat(storageConnectionProvider,
                vertexExecutionOptions);
        DataSource<Row> rowDataSource = env.createInput(inputRowFormat);
        rowDataSource.print();
        System.out.println("rowDataSource count: " + rowDataSource.count());

        // get Nebula vertex data in nebula TableRow format
        NebulaInputTableRowFormat inputFormat =
                new NebulaInputTableRowFormat(storageConnectionProvider,
                vertexExecutionOptions);
        DataSource<BaseTableRow> dataSource = env.createInput(inputFormat);
        dataSource.print();
        System.out.println("datasource count: " + dataSource.count());
    }

    /**
     * read Nebula Graph edge as batch data source
     */
    public static void nebulaEdgeBatchSource() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // get Nebula edge data in flink Row format
        NebulaInputRowFormat inputFormat = new NebulaInputRowFormat(storageConnectionProvider,
                edgeExecutionOptions);
        DataSource<Row> dataSourceRow = env.createInput(inputFormat);
        dataSourceRow.print();
        System.out.println("datasource count: " + dataSourceRow.count());

        // get Nebula edge data in Nebula TableRow format
        NebulaInputTableRowFormat inputTableFormat =
                new NebulaInputTableRowFormat(storageConnectionProvider,
                edgeExecutionOptions);
        DataSource<BaseTableRow> dataSourceTableRow = env.createInput(inputTableFormat);
        dataSourceTableRow.print();
        System.out.println("datasource count: " + dataSourceTableRow.count());
    }
}
