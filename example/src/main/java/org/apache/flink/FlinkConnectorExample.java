/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package org.apache.flink;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.connector.nebula.connection.NebulaClientOptions;
import org.apache.flink.connector.nebula.connection.NebulaGraphConnectionProvider;
import org.apache.flink.connector.nebula.connection.NebulaMetaConnectionProvider;
import org.apache.flink.connector.nebula.sink.NebulaBatchOutputFormat;
import org.apache.flink.connector.nebula.sink.NebulaSinkFunction;
import org.apache.flink.connector.nebula.statement.EdgeExecutionOptions;
import org.apache.flink.connector.nebula.statement.ExecutionOptions;
import org.apache.flink.connector.nebula.statement.VertexExecutionOptions;
import org.apache.flink.connector.nebula.utils.SslSighType;
import org.apache.flink.connector.nebula.utils.WriteModeEnum;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * make sure your nebula graph has create Space
 */
public class FlinkConnectorExample {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkConnectorExample.class);

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<List<String>> playerSource = constructVertexSourceData(env);
        sinkVertexData(env, playerSource);
        updateVertexData(env, playerSource);
        deleteVertexData(env, playerSource);
        DataStream<List<String>> friendSource = constructEdgeSourceData(env);
        sinkEdgeData(env, friendSource);
    }

    /**
     * construct flink data source
     */
    public static DataStream<List<String>> constructVertexSourceData(
            StreamExecutionEnvironment env) {
        List<List<String>> player = new ArrayList<>();
        List<String> fields1 = Arrays.asList("15", "Bob", "38");
        List<String> fields2 = Arrays.asList("16", "Tina", "39");
        List<String> fields3 = Arrays.asList("17", "Jena", "30");
        List<String> fields4 = Arrays.asList("18", "Tom", "30");
        List<String> fields5 = Arrays.asList("19", "Viki", "35");
        List<String> fields6 = Arrays.asList("20", "Jime", "33");
        List<String> fields7 = Arrays.asList("21", "Jhon", "36");
        List<String> fields8 = Arrays.asList("22", "Crea", "30");
        player.add(fields1);
        player.add(fields2);
        player.add(fields3);
        player.add(fields4);
        player.add(fields5);
        player.add(fields6);
        player.add(fields7);
        player.add(fields8);
        DataStream<List<String>> playerSource = env.fromCollection(player);
        return playerSource;
    }

    private static NebulaClientOptions getClientOptions() {
        // not enable ssl for graph
        NebulaClientOptions nebulaClientOptions =
                new NebulaClientOptions.NebulaClientOptionsBuilder()
                        .setGraphAddress("127.0.0.1:9669")
                        .setMetaAddress("127.0.0.1:9559")
                        .build();

        // enable ssl for graph with CA ssl sign
        NebulaClientOptions nebulaClientOptionsWithCaSsl =
                new NebulaClientOptions.NebulaClientOptionsBuilder()
                        .setGraphAddress("127.0.0.1:9669")
                        .setMetaAddress("127.0.0.1:9559")
                        .setEnableMetaSsl(true)
                        .setSslSignType(SslSighType.CA)
                        .setCaSignParam("src/main/resources/ssl/casigned.pem",
                                "src/main/resources/ssl/casigned.crt",
                                "src/main/resources/ssl/casigned.key")
                        .build();

        // enable ssl for graph with Self ssl sign
        NebulaClientOptions nebulaClientOptionsWithSelfSsl =
                new NebulaClientOptions.NebulaClientOptionsBuilder()
                        .setGraphAddress("127.0.0.1:9669")
                        .setMetaAddress("127.0.0.1:9559")
                        .setEnableMetaSsl(true)
                        .setSslSignType(SslSighType.SELF)
                        .setSelfSignParam("src/main/resources/ssl/selfsigned.pem",
                                "src/main/resources/ssl/selfsigned.key",
                                "vesoft")
                        .build();
        return nebulaClientOptions;
    }

    /**
     * sink Nebula Graph with default INSERT mode
     */
    public static void sinkVertexData(StreamExecutionEnvironment env,
                                      DataStream<List<String>> playerSource) {
        NebulaClientOptions nebulaClientOptions = getClientOptions();
        NebulaGraphConnectionProvider graphConnectionProvider =
                new NebulaGraphConnectionProvider(nebulaClientOptions);
        NebulaMetaConnectionProvider metaConnectionProvider =
                new NebulaMetaConnectionProvider(nebulaClientOptions);

        ExecutionOptions executionOptions = new VertexExecutionOptions.ExecutionOptionBuilder()
                .setGraphSpace("flinkSink")
                .setTag("player")
                .setIdIndex(0)
                .setFields(Arrays.asList("name", "age"))
                .setPositions(Arrays.asList(1, 2))
                .setBatch(2)
                .builder();

        NebulaBatchOutputFormat outPutFormat =
                new NebulaBatchOutputFormat(graphConnectionProvider, metaConnectionProvider)
                        .setExecutionOptions(executionOptions);
        NebulaSinkFunction nebulaSinkFunction = new NebulaSinkFunction(outPutFormat);
        DataStream<Row> dataStream = playerSource.map(row -> {
            org.apache.flink.types.Row record = new org.apache.flink.types.Row(row.size());
            for (int i = 0; i < row.size(); i++) {
                record.setField(i, row.get(i));
            }
            return record;
        });
        dataStream.addSink(nebulaSinkFunction);
        try {
            env.execute("Write Nebula");
        } catch (Exception e) {
            LOG.error("error when write Nebula Graph, ", e);
            System.exit(-1);
        }
    }

    /**
     * sink Nebula Graph with UPDATE mode
     */
    public static void updateVertexData(StreamExecutionEnvironment env,
                                        DataStream<List<String>> playerSource) {
        NebulaClientOptions nebulaClientOptions = getClientOptions();
        NebulaGraphConnectionProvider graphConnectionProvider =
                new NebulaGraphConnectionProvider(nebulaClientOptions);
        NebulaMetaConnectionProvider metaConnectionProvider =
                new NebulaMetaConnectionProvider(nebulaClientOptions);

        ExecutionOptions executionOptions = new VertexExecutionOptions.ExecutionOptionBuilder()
                .setGraphSpace("flinkSink")
                .setTag("player")
                .setIdIndex(0)
                .setFields(Arrays.asList("name", "age"))
                .setPositions(Arrays.asList(1, 2))
                .setWriteMode(WriteModeEnum.UPDATE)
                .setBatch(2)
                .builder();

        NebulaBatchOutputFormat outPutFormat =
                new NebulaBatchOutputFormat(graphConnectionProvider, metaConnectionProvider)
                        .setExecutionOptions(executionOptions);
        NebulaSinkFunction nebulaSinkFunction = new NebulaSinkFunction(outPutFormat);
        DataStream<Row> dataStream = playerSource.map(row -> {
            org.apache.flink.types.Row record = new org.apache.flink.types.Row(row.size());
            for (int i = 0; i < row.size(); i++) {
                record.setField(i, row.get(i));
            }
            return record;
        });
        dataStream.addSink(nebulaSinkFunction);
        try {
            env.execute("Update Nebula Vertex");
        } catch (Exception e) {
            LOG.error("error when update Nebula Graph Vertex, ", e);
            System.exit(-1);
        }
    }

    /**
     * sink Nebula Graph with DELETE mode
     */
    public static void deleteVertexData(StreamExecutionEnvironment env,
                                        DataStream<List<String>> playerSource) {
        NebulaClientOptions nebulaClientOptions = getClientOptions();
        NebulaGraphConnectionProvider graphConnectionProvider =
                new NebulaGraphConnectionProvider(nebulaClientOptions);
        NebulaMetaConnectionProvider metaConnectionProvider =
                new NebulaMetaConnectionProvider(nebulaClientOptions);

        ExecutionOptions executionOptions = new VertexExecutionOptions.ExecutionOptionBuilder()
                .setGraphSpace("flinkSink")
                .setTag("player")
                .setIdIndex(0)
                .setFields(Arrays.asList("name", "age"))
                .setPositions(Arrays.asList(1, 2))
                .setWriteMode(WriteModeEnum.DELETE)
                .setBatch(2)
                .builder();

        NebulaBatchOutputFormat outPutFormat =
                new NebulaBatchOutputFormat(graphConnectionProvider, metaConnectionProvider)
                        .setExecutionOptions(executionOptions);
        NebulaSinkFunction nebulaSinkFunction = new NebulaSinkFunction(outPutFormat);
        DataStream<Row> dataStream = playerSource.map(row -> {
            org.apache.flink.types.Row record = new org.apache.flink.types.Row(row.size());
            for (int i = 0; i < row.size(); i++) {
                record.setField(i, row.get(i));
            }
            return record;
        });
        dataStream.addSink(nebulaSinkFunction);
        try {
            env.execute("Update Nebula Vertex");
        } catch (Exception e) {
            LOG.error("error when update Nebula Graph Vertex, ", e);
            System.exit(-1);
        }
    }


    /**
     * construct flink data source
     */
    public static DataStream<List<String>> constructEdgeSourceData(StreamExecutionEnvironment env) {
        List<List<String>> friend = new ArrayList<>();
        List<String> fields1 = Arrays.asList("nicole", "Tom", "15", "18.0", "2019-05-01");
        List<String> fields2 = Arrays.asList("Tina", "John", "16", "19.0", "2018-03-08");
        List<String> fields3 = Arrays.asList("Bob", "Lisa", "17", "20.0", "2015-04-01");
        List<String> fields4 = Arrays.asList("Tom", "Lisa", "18", "20.0", "2016-04-01");
        List<String> fields5 = Arrays.asList("Jime", "John", "19", "20.0", "2017-04-01");
        List<String> fields6 = Arrays.asList("Tim", "Bob", "20", "20.0", "2020-04-01");
        friend.add(fields1);
        friend.add(fields2);
        friend.add(fields3);
        friend.add(fields4);
        friend.add(fields5);
        friend.add(fields6);
        DataStream<List<String>> playerSource = env.fromCollection(friend);
        return playerSource;
    }

    /**
     * sink Nebula Graph
     */
    public static void sinkEdgeData(StreamExecutionEnvironment env,
                                    DataStream<List<String>> playerSource) {
        NebulaClientOptions nebulaClientOptions = getClientOptions();
        NebulaGraphConnectionProvider graphConnectionProvider =
                new NebulaGraphConnectionProvider(nebulaClientOptions);
        NebulaMetaConnectionProvider metaConnectionProvider =
                new NebulaMetaConnectionProvider(nebulaClientOptions);

        ExecutionOptions executionOptions = new EdgeExecutionOptions.ExecutionOptionBuilder()
                .setGraphSpace("flinkSink")
                .setEdge("friend")
                .setSrcIndex(0)
                .setDstIndex(1)
                .setRankIndex(2)
                .setFields(Arrays.asList("src", "dst", "degree", "start"))
                .setPositions(Arrays.asList(0, 1, 3, 4))
                .setBatch(2)
                .builder();

        NebulaBatchOutputFormat outPutFormat =
                new NebulaBatchOutputFormat(graphConnectionProvider, metaConnectionProvider)
                        .setExecutionOptions(executionOptions);
        NebulaSinkFunction nebulaSinkFunction = new NebulaSinkFunction(outPutFormat);
        DataStream<Row> dataStream = playerSource.map(row -> {
            org.apache.flink.types.Row record = new org.apache.flink.types.Row(row.size());
            for (int i = 0; i < row.size(); i++) {
                record.setField(i, row.get(i));
            }
            return record;
        });
        dataStream.addSink(nebulaSinkFunction);
        try {
            env.execute("Write Nebula");
        } catch (Exception e) {
            LOG.error("error when write Nebula Graph, ", e);
            System.exit(-1);
        }
    }
}
