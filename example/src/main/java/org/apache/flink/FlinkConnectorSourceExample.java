/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package org.apache.flink;

import com.vesoft.nebula.client.storage.data.BaseTableRow;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.connector.nebula.connection.NebulaClientOptions;
import org.apache.flink.connector.nebula.connection.NebulaStorageConnectionProvider;
import org.apache.flink.connector.nebula.source.NebulaInputFormat;
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

    public static void main(String[] args) throws Exception {
        initConfig();

        //nebulaVertexStreamSource();
        //nebulaVertexBatchSource();

        //nebulaEdgeStreamSource();
        nebulaEdgeBatchSource();

        System.exit(0);
    }

    public static void initConfig(){
        NebulaClientOptions nebulaClientOptions =
                new NebulaClientOptions.NebulaClientOptionsBuilder()
                        .setMetaAddress("192.168.8.172:45509")
                        .build();
        storageConnectionProvider =
                new NebulaStorageConnectionProvider(nebulaClientOptions);

        // read no property
        vertexExecutionOptions = new VertexExecutionOptions.ExecutionOptionBuilder()
                .setGraphSpace("flinkSink")
                .setTag("player")
                .setNoColumn(true)
                .setFields(Arrays.asList("name", "age"))
                .setLimit(100)
                .builder();


        // read specific properties
        // if you want to read all properties, config: setFields(Arrays.asList())
        edgeExecutionOptions = new EdgeExecutionOptions.ExecutionOptionBuilder()
                .setGraphSpace("flinkSink")
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
            List<Object> values = row.getValues();
            Row record = new Row(1);
            record.setField(0, values.get(0));
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
        env.setParallelism(1);

        NebulaSourceFunction sourceFunction = new NebulaSourceFunction(storageConnectionProvider)
                .setExecutionOptions(edgeExecutionOptions);
        DataStreamSource<BaseTableRow> dataStreamSource = env.addSource(sourceFunction);

        dataStreamSource.map(row -> {
            List<Object> values = row.getValues();
            Row record = new Row(6);
            record.setField(0, values.get(0));
            record.setField(1, values.get(1));
            record.setField(2, values.get(2));
            record.setField(3, values.get(3));
            record.setField(4, values.get(4));
            record.setField(5, values.get(5));
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

        NebulaInputFormat inputFormat = new NebulaInputFormat(storageConnectionProvider,
                vertexExecutionOptions);
        DataSource<Row> dataSource = env.createInput(inputFormat);
        dataSource.print();
        System.out.println("datasource count: " + dataSource.count());
        System.exit(0);
    }

    /**
     * read Nebula Graph edge as batch data source
     */
    public static void nebulaEdgeBatchSource() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        NebulaInputFormat inputFormat = new NebulaInputFormat(storageConnectionProvider,
                edgeExecutionOptions);
        DataSource<Row> dataSource = env.createInput(inputFormat);
        dataSource.print();
        System.out.println("datasource count: " + dataSource.count());
    }

}
