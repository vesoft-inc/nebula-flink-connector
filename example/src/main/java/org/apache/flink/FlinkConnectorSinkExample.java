/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.connector.nebula.connection.NebulaClientOptions;
import org.apache.flink.connector.nebula.connection.NebulaGraphConnectionProvider;
import org.apache.flink.connector.nebula.connection.NebulaMetaConnectionProvider;
import org.apache.flink.connector.nebula.sink.NebulaEdgeBatchOutputFormat;
import org.apache.flink.connector.nebula.sink.NebulaSinkFunction;
import org.apache.flink.connector.nebula.sink.NebulaVertexBatchOutputFormat;
import org.apache.flink.connector.nebula.statement.EdgeExecutionOptions;
import org.apache.flink.connector.nebula.statement.VertexExecutionOptions;
import org.apache.flink.connector.nebula.utils.SSLSignType;
import org.apache.flink.connector.nebula.utils.WriteModeEnum;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * make sure your nebula graph has create Space, space schema:
 *
 * <p>"CREATE SPACE `flinkSink` (partition_num = 100, replica_factor = 3, charset = utf8,
 * collate = utf8_bin, vid_type = INT64, atomic_edge = false)"
 *
 * <p>"USE `flinkSink`"
 *
 * <p>"CREATE TAG IF NOT EXISTS person(col1 string, col2 fixed_string(8), col3 int8, col4 int16,
 * col5 int32, col6 int64, col7 date, col8 datetime, col9 timestamp, col10 bool, col11 double,
 * col12 float, col13 time, col14 geography);"
 *
 * <p>"CREATE EDGE IF NOT EXISTS friend(col1 string, col2 fixed_string(8), col3 int8, col4 int16,
 * col5 int32, col6 int64, col7 date, col8 datetime, col9 timestamp, col10 bool, col11 double,
 * col12 float, col13 time, col14 geography);"
 */
public class FlinkConnectorSinkExample {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkConnectorSinkExample.class);

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(5);
        DataStream<List<String>> playerSource = constructVertexSourceData(env);
        sinkVertexData(env, playerSource);
        updateVertexData(env, playerSource);
        deleteVertexData(env, playerSource);

        DataStream<List<String>> friendSource = constructEdgeSourceData(env);
        sinkEdgeData(env, friendSource);
        updateEdgeData(env, friendSource);
        deleteEdgeData(env, friendSource);
    }

    /**
     * construct flink data source
     */
    public static DataStream<List<String>> constructVertexSourceData(
            StreamExecutionEnvironment env) {
        List<List<String>> player = new ArrayList<>();
        List<String> fields1 = Arrays.asList("61", "aba", "abcdefgh", "1", "1111", "22222",
                "6412233", "2019-01-01", "2019-01-01T12:12:12", "435463424", "false", "1.2", "1.0",
                "11:12:12", "POINT(1 3)");
        List<String> fields2 = Arrays.asList("62", "aba", "abcdefgh", "1", "1111", "22222",
                "6412233", "2019-01-01", "2019-01-01T12:12:12", "435463424", "false", "1.2", "1.0",
                "11:12:12", "POINT(1 3)");
        List<String> fields3 = Arrays.asList("63", "aba", "abcdefgh", "1", "1111", "22222",
                "6412233", "2019-01-01", "2019-01-01T12:12:12", "435463424", "false", "1.2", "1.0",
                "11:12:12", "POINT(1 3)");
        List<String> fields4 = Arrays.asList("64", "aba", "abcdefgh", "1", "1111", "22222",
                "6412233", "2019-01-01", "2019-01-01T12:12:12", "435463424", "false", "1.2", "1.0",
                "11:12:12", "LINESTRING(1 3,2 4)");
        List<String> fields5 = Arrays.asList("65", "aba", "abcdefgh", "1", "1111", "22222",
                "6412233", "2019-01-01", "2019-01-01T12:12:12", "435463424", "false", "1.2", "1.0",
                "11:12:12", "LINESTRING(1 3,2 4)");
        List<String> fields6 = Arrays.asList("66", "aba", "abcdefgh", "1", "1111", "22222",
                "6412233", "2019-01-01", "2019-01-01T12:12:12", "435463424", "false", "1.2", "1.0",
                "11:12:12", "LINESTRING(1 3,2 4)");
        List<String> fields7 = Arrays.asList("67", "李四", "abcdefgh", "1", "1111", "22222",
                "6412233", "2019-01-01", "2019-01-01T12:12:12", "435463424", "true", "1.2", "1.0",
                "11:12:12", "polygon((0 1,1 2,2 3,0 1))");
        List<String> fields8 = Arrays.asList("68", "aba", "张三", "1", "1111", "22222", "6412233",
                "2019-01-01", "2019-01-01T12:12:12", "435463424", "true", "1.2", "1.0", "11:12:12",
                "POLYGON((0 1,1 2,2 3,0 1))");
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
                        .setEnableGraphSSL(true)
                        .setSSLSignType(SSLSignType.CA)
                        .setCaSignParam("example/src/main/resources/ssl/casigned.pem",
                                "example/src/main/resources/ssl/casigned.crt",
                                "example/src/main/resources/ssl/casigned.key")
                        .build();

        // enable ssl for graph with Self ssl sign
        NebulaClientOptions nebulaClientOptionsWithSelfSsl =
                new NebulaClientOptions.NebulaClientOptionsBuilder()
                        .setGraphAddress("127.0.0.1:9669")
                        .setMetaAddress("127.0.0.1:9559")
                        .setEnableGraphSSL(true)
                        .setSSLSignType(SSLSignType.SELF)
                        .setSelfSignParam("example/src/main/resources/ssl/selfsigned.pem",
                                "example/src/main/resources/ssl/selfsigned.key",
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

        VertexExecutionOptions executionOptions =
                new VertexExecutionOptions.ExecutionOptionBuilder()
                        .setGraphSpace("flinkSink")
                        .setTag("person")
                        .setIdIndex(0)
                        .setFields(Arrays.asList("col1", "col2", "col3", "col4", "col5", "col6",
                                "col7", "col8", "col9", "col10", "col11", "col12",
                                "col13", "col14"))
                        .setPositions(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14))
                        .setBatchSize(2)
                        .build();

        NebulaVertexBatchOutputFormat outputFormat = new NebulaVertexBatchOutputFormat(
                graphConnectionProvider, metaConnectionProvider, executionOptions);
        NebulaSinkFunction<Row> nebulaSinkFunction = new NebulaSinkFunction<>(outputFormat);
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

        VertexExecutionOptions executionOptions =
                new VertexExecutionOptions.ExecutionOptionBuilder()
                        .setGraphSpace("flinkSink")
                        .setTag("person")
                        .setIdIndex(0)
                        .setFields(Arrays.asList("col1", "col2"))
                        .setPositions(Arrays.asList(1, 2))
                        .setWriteMode(WriteModeEnum.UPDATE)
                        .setBatchSize(2)
                        .build();

        NebulaVertexBatchOutputFormat outputFormat = new NebulaVertexBatchOutputFormat(
                graphConnectionProvider, metaConnectionProvider, executionOptions);
        NebulaSinkFunction<Row> nebulaSinkFunction = new NebulaSinkFunction<>(outputFormat);
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

        VertexExecutionOptions executionOptions =
                new VertexExecutionOptions.ExecutionOptionBuilder()
                        .setGraphSpace("flinkSink")
                        .setTag("person")
                        .setIdIndex(0)
                        .setFields(Arrays.asList("col1", "col2"))
                        .setPositions(Arrays.asList(1, 2))
                        .setWriteMode(WriteModeEnum.DELETE)
                        .setBatchSize(2)
                        .build();

        NebulaVertexBatchOutputFormat outputFormat = new NebulaVertexBatchOutputFormat(
                graphConnectionProvider, metaConnectionProvider, executionOptions);
        NebulaSinkFunction<Row> nebulaSinkFunction = new NebulaSinkFunction<>(outputFormat);
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
        List<String> fields1 = Arrays.asList("61", "62", "aba", "abcdefgh", "1", "1111", "22222",
                "6412233", "2019-01-01", "2019-01-01T12:12:12", "435463424", "false", "1.2", "1.0",
                "11:12:12", "POINT(1 3)");
        List<String> fields2 = Arrays.asList("62", "63", "aba", "abcdefgh", "1", "1111", "22222",
                "6412233", "2019-01-01", "2019-01-01T12:12:12", "435463424", "false", "1.2", "1.0",
                "11:12:12", "POINT(1 3)");
        List<String> fields3 = Arrays.asList("63", "64", "aba", "abcdefgh", "1", "1111", "22222",
                "6412233", "2019-01-01", "2019-01-01T12:12:12", "435463424", "false", "1.2", "1.0",
                "11:12:12", "POINT(1 3)");
        List<String> fields4 = Arrays.asList("64", "65", "aba", "abcdefgh", "1", "1111", "22222",
                "6412233", "2019-01-01", "2019-01-01T12:12:12", "435463424", "false", "1.2", "1.0",
                "11:12:12", "LINESTRING(1 3,2 4)");
        List<String> fields5 = Arrays.asList("65", "66", "aba", "abcdefgh", "1", "1111", "22222",
                "6412233", "2019-01-01", "2019-01-01T12:12:12", "435463424", "false", "1.2", "1.0",
                "11:12:12", "LINESTRING(1 3,2 4)");
        List<String> fields6 = Arrays.asList("66", "67", "aba", "abcdefgh", "1", "1111", "22222",
                "6412233", "2019-01-01", "2019-01-01T12:12:12", "435463424", "false", "1.2", "1.0",
                "11:12:12", "LINESTRING(1 3,2 4)");
        List<String> fields7 = Arrays.asList("67", "68", "李四", "abcdefgh", "1", "1111", "22222",
                "6412233", "2019-01-01", "2019-01-01T12:12:12", "435463424", "true", "1.2", "1.0",
                "11:12:12", "polygon((0 1,1 2,2 3,0 1))");
        List<String> fields8 = Arrays.asList("68", "61", "aba", "张三", "1", "1111", "22222",
                "6412233",
                "2019-01-01", "2019-01-01T12:12:12", "435463424", "true", "1.2", "1.0", "11:12:12",
                "POLYGON((0 1,1 2,2 3,0 1))");
        friend.add(fields1);
        friend.add(fields2);
        friend.add(fields3);
        friend.add(fields4);
        friend.add(fields5);
        friend.add(fields6);
        friend.add(fields7);
        friend.add(fields8);
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

        EdgeExecutionOptions executionOptions = new EdgeExecutionOptions.ExecutionOptionBuilder()
                .setGraphSpace("flinkSink")
                .setEdge("friend")
                .setSrcIndex(0)
                .setDstIndex(1)
                .setRankIndex(4)
                .setFields(Arrays.asList("col1", "col2", "col3", "col4", "col5", "col6", "col7",
                        "col8", "col9", "col10", "col11", "col12", "col13", "col14"))
                .setPositions(Arrays.asList(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15))
                .setBatchSize(2)
                .build();

        NebulaEdgeBatchOutputFormat outputFormat = new NebulaEdgeBatchOutputFormat(
                graphConnectionProvider, metaConnectionProvider, executionOptions);
        NebulaSinkFunction<Row> nebulaSinkFunction = new NebulaSinkFunction<>(outputFormat);
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
    public static void updateEdgeData(StreamExecutionEnvironment env,
                                      DataStream<List<String>> playerSource) {
        NebulaClientOptions nebulaClientOptions = getClientOptions();
        NebulaGraphConnectionProvider graphConnectionProvider =
                new NebulaGraphConnectionProvider(nebulaClientOptions);
        NebulaMetaConnectionProvider metaConnectionProvider =
                new NebulaMetaConnectionProvider(nebulaClientOptions);

        EdgeExecutionOptions executionOptions = new EdgeExecutionOptions.ExecutionOptionBuilder()
                .setGraphSpace("flinkSink")
                .setEdge("friend")
                .setSrcIndex(0)
                .setDstIndex(1)
                .setRankIndex(4)
                .setFields(Arrays.asList("col1", "col2"))
                .setPositions(Arrays.asList(2, 3))
                .setWriteMode(WriteModeEnum.UPDATE)
                .setBatchSize(2)
                .build();

        NebulaEdgeBatchOutputFormat outputFormat = new NebulaEdgeBatchOutputFormat(
                graphConnectionProvider, metaConnectionProvider, executionOptions);
        NebulaSinkFunction<Row> nebulaSinkFunction = new NebulaSinkFunction<>(outputFormat);
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
    public static void deleteEdgeData(StreamExecutionEnvironment env,
                                      DataStream<List<String>> playerSource) {
        NebulaClientOptions nebulaClientOptions = getClientOptions();
        NebulaGraphConnectionProvider graphConnectionProvider =
                new NebulaGraphConnectionProvider(nebulaClientOptions);
        NebulaMetaConnectionProvider metaConnectionProvider =
                new NebulaMetaConnectionProvider(nebulaClientOptions);

        EdgeExecutionOptions executionOptions = new EdgeExecutionOptions.ExecutionOptionBuilder()
                .setGraphSpace("flinkSink")
                .setEdge("friend")
                .setSrcIndex(0)
                .setDstIndex(1)
                .setRankIndex(4)
                .setFields(Arrays.asList("col1", "col2"))
                .setPositions(Arrays.asList(2, 3))
                .setWriteMode(WriteModeEnum.DELETE)
                .setBatchSize(2)
                .build();

        NebulaEdgeBatchOutputFormat outputFormat = new NebulaEdgeBatchOutputFormat(
                graphConnectionProvider, metaConnectionProvider, executionOptions);
        NebulaSinkFunction<Row> nebulaSinkFunction = new NebulaSinkFunction<>(outputFormat);
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
}
