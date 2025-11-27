/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink;

import com.vesoft.nebula.driver.graph.data.ResultSet;
import com.vesoft.nebula.driver.graph.net.NebulaClient;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.connector.nebula.options.ConnectionOptions;
import org.apache.flink.connector.nebula.options.SinkEdgeOptions;
import org.apache.flink.connector.nebula.options.SinkNodeOptions;
import org.apache.flink.connector.nebula.sink.NebulaEdgeBatchOutputFormat;
import org.apache.flink.connector.nebula.sink.NebulaNodeBatchOutputFormat;
import org.apache.flink.connector.nebula.sink.NebulaSinkFunction;
import org.apache.flink.connector.nebula.utils.WriteModeEnum;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkConnectorSinkExample {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkConnectorSinkExample.class);

    public static void main(String[] args) {
        prepareGraph();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(5);
        DataStream<List<String>> playerSource = constructNodeSourceData(env);
        sinkNodeData(env, playerSource);
        updateNodeData(env, playerSource);

        DataStream<List<String>> friendSource = constructEdgeSourceData(env);
        sinkEdgeData(env, friendSource);
        updateEdgeData(env, friendSource);

        deleteEdgeData(env, friendSource);
        deleteNodeData(env, playerSource);
    }

    private static void prepareGraph() {
        String graphType = "CREATE GRAPH TYPE IF NOT EXISTS flinkSinkType AS{\n"
                + "NODE TYPE person(LABEL person{col1 string primary key, col2 string, col3 int8,"
                + " col4 int16,col5 int32, col6 int64, col7 date, col8 local datetime, "
                + "col9 local time, col10 bool, col11 double, col12 float, col13 zoned time}),\n"
                + "EDGE TYPE friend(person)-[LABEL friend{col1 string, col2 string, col3 int8, "
                + "col4 int16, col5 int32, col6 int64, col7 date, col8 local datetime, "
                + "col9 local time, col10 bool, col11 double,col12 float, col13 zoned time}]"
                + "->(person)\n"
                + " }";
        String       graph  = "CREATE GRAPH IF NOT EXISTS flinkSink TYPED flinkSinkType";
        NebulaClient client = null;
        try {
            client = NebulaClient
                    .builder("192.168.8.6:3820", "root", "NebulaGraph01")
                    .build();
            ResultSet res = client.execute(graphType);
            if (!res.isSucceeded()) {
                LOG.error("create graph type failed:" + res.getErrorMessage());
                System.exit(1);
            }
            res = client.execute(graph);
            if (!res.isSucceeded()) {
                LOG.error("create graph failed:" + res.getErrorMessage());
                System.exit(1);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        } finally {
            if (client != null) {
                client.close();
            }
        }
        LOG.info("prepare sink graph finished!");
    }

    /**
     * construct flink data source
     */
    public static DataStream<List<String>> constructNodeSourceData(
            StreamExecutionEnvironment env) {
        List<List<String>> player = new ArrayList<>();
        List<String> fields1 = Arrays.asList("61", "aba", "1", "1111", "22222", "6412233",
                                             "2019-01-01", "2019-01-01T12:12:12", "10:10:10",
                                             "false", "1.2", "1.0", "11:12:12");
        List<String> fields2 = Arrays.asList("62", "aba", "1", "1111", "22222", "6412233",
                                             "2019-01-01", "2019-01-01T12:12:12", "10:10:10",
                                             "false", "1.2", "1.0", "11:12:12");
        List<String> fields3 = Arrays.asList("63", "aba", "1", "1111", "22222", "6412233",
                                             "2019-01-01", "2019-01-01T12:12:12", "10:10:10",
                                             "false", "1.2", "1.0", "11:12:12");
        List<String> fields4 = Arrays.asList("64", "aba", "1", "1111", "22222", "6412233",
                                             "2019-01-01", "2019-01-01T12:12:12", "10:10:10",
                                             "false", "1.2", "1.0", "11:12:12");
        List<String> fields5 = Arrays.asList("65", "aba", "1", "1111", "22222", "6412233",
                                             "2019-01-01", "2019-01-01T12:12:12", "10:10:10",
                                             "false", "1.2", "1.0", "11:12:12");
        List<String> fields6 = Arrays.asList("66", "aba", "1", "1111", "22222", "6412233",
                                             "2019-01-01", "2019-01-01T12:12:12", "10:10:10",
                                             "false", "1.2", "1.0", "11:12:12");
        List<String> fields7 = Arrays.asList("67", "李四", "1", "1111", "22222", "6412233",
                                             "2019-01-01", "2019-01-01T12:12:12", "10:10:10",
                                             "true", "1.2", "1.0", "11:12:12");
        List<String> fields8 = Arrays.asList("68", "张三", "1", "1111", "22222", "6412233",
                                             "2019-01-01", "2019-01-01T12:12:12", "10:10:10",
                                             "true", "1.2", "1.0", "11:12:12");
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

    private static ConnectionOptions getConnectionOptions() {
        ConnectionOptions connectionOptions = ConnectionOptions
                .builder()
                .withGraphAddress("192.168.8.6:3820")
                .withUser("root")
                .withPassword("NebulaGraph01")
                .withZonedTimeFormat("%H:%M:%S")
                .build();
        return connectionOptions;
    }

    /**
     * sink Nebula Graph with default INSERT mode
     */
    public static void sinkNodeData(StreamExecutionEnvironment env,
                                    DataStream<List<String>> playerSource) {
        ConnectionOptions connectionOptions = getConnectionOptions();

        SinkNodeOptions sinkNodeOptions =
                SinkNodeOptions.builder()
                        .withGraphName("flinkSink")
                        .withNodeType("person")
                        .withFlinkFields(Arrays.asList("c0", "c1", "c2", "c3", "c4", "c5", "c6",
                                                       "c7", "c8", "c9", "c10", "c11", "c12"))
                        .withNebulaFields(Arrays.asList("col1", "col2", "col3", "col4", "col5",
                                                        "col6", "col7", "col8", "col9", "col10",
                                                        "col11", "col12", "col13"))
                        .withWriteMode(WriteModeEnum.INSERTREPLACE)
                        .withBatchSize(10)
                        .build();


        NebulaNodeBatchOutputFormat outputFormat =
                new NebulaNodeBatchOutputFormat(connectionOptions, sinkNodeOptions);
        NebulaSinkFunction<Row> nebulaSinkFunction = new NebulaSinkFunction<>(outputFormat);
        DataStream<Row> dataStream = playerSource.map(row -> {
            org.apache.flink.types.Row record = Row.withNames();
            for (int i = 0; i < row.size(); i++) {
                record.setField("c" + i, row.get(i));
            }
            return record;
        });
        dataStream.print().name("print");
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
    public static void updateNodeData(StreamExecutionEnvironment env,
                                      DataStream<List<String>> playerSource) {
        ConnectionOptions connectionOptions = getConnectionOptions();
        SinkNodeOptions sinkNodeOptions =
                SinkNodeOptions.builder()
                        .withGraphName("flinkSink")
                        .withNodeType("person")
                        .withFlinkFields(Arrays.asList("c0", "c1", "c2", "c3", "c4", "c5", "c6",
                                                       "c7", "c8", "c9", "c10", "c11", "c12"))
                        .withNebulaFields(Arrays.asList("col1", "col2", "col3", "col4", "col5",
                                                        "col6", "col7", "col8", "col9", "col10",
                                                        "col11", "col12", "col13"))
                        .withWriteMode(WriteModeEnum.UPDATE)
                        .withBatchSize(2)
                        .build();

        NebulaNodeBatchOutputFormat outputFormat = new NebulaNodeBatchOutputFormat(
                connectionOptions, sinkNodeOptions);
        NebulaSinkFunction<Row> nebulaSinkFunction = new NebulaSinkFunction<>(outputFormat);
        DataStream<Row> dataStream = playerSource.map(row -> {
            org.apache.flink.types.Row record = Row.withNames();
            for (int i = 0; i < row.size(); i++) {
                record.setField("c" + i, row.get(i));
            }
            return record;
        });
        dataStream.addSink(nebulaSinkFunction);
        try {
            env.execute("Update Nebula Node");
        } catch (Exception e) {
            LOG.error("error when update Nebula Graph Node, ", e);
            System.exit(-1);
        }
    }

    /**
     * sink Nebula Graph with DELETE mode
     */
    public static void deleteNodeData(StreamExecutionEnvironment env,
                                      DataStream<List<String>> playerSource) {
        ConnectionOptions connectionOptions = getConnectionOptions();
        SinkNodeOptions sinkNodeOptions =
                SinkNodeOptions.builder()
                        .withGraphName("flinkSink")
                        .withNodeType("person")
                        .withFlinkFields(Arrays.asList("c0", "c1", "c2", "c3", "c4", "c5", "c6",
                                                       "c7", "c8", "c9", "c10", "c11", "c12"))
                        .withNebulaFields(Arrays.asList("col1", "col2", "col3", "col4", "col5",
                                                        "col6", "col7", "col8", "col9", "col10",
                                                        "col11", "col12", "col13"))
                        .withWriteMode(WriteModeEnum.DETACHDELETE)
                        .withBatchSize(2)
                        .build();

        NebulaNodeBatchOutputFormat outputFormat = new NebulaNodeBatchOutputFormat(
                connectionOptions, sinkNodeOptions);
        NebulaSinkFunction<Row> nebulaSinkFunction = new NebulaSinkFunction<>(outputFormat);
        DataStream<Row> dataStream = playerSource.map(row -> {
            org.apache.flink.types.Row record = Row.withNames();
            for (int i = 0; i < row.size(); i++) {
                record.setField("c" + i, row.get(i));
            }
            return record;
        });
        dataStream.addSink(nebulaSinkFunction);
        try {
            env.execute("Delete Nebula Node");
        } catch (Exception e) {
            LOG.error("error when Delete Nebula Graph Node, ", e);
            System.exit(-1);
        }
    }


    /**
     * construct flink data source
     */
    public static DataStream<List<String>> constructEdgeSourceData(StreamExecutionEnvironment env) {
        List<List<String>> friend = new ArrayList<>();
        List<String> fields1 = Arrays.asList("61", "62", "aba", "abcdefgh", "1", "1111", "22222",
                                             "6412233", "2019-01-01", "2019-01-01T12:12:12",
                                             "15:10:00", "false", "1.2", "1.0", "11:12:12");
        List<String> fields2 = Arrays.asList("62", "63", "aba", "abcdefgh", "1", "1111", "22222",
                                             "6412233", "2019-01-01", "2019-01-01T12:12:12",
                                             "15:10:00", "false", "1.2", "1.0", "11:12:12");
        List<String> fields3 = Arrays.asList("63", "64", "aba", "abcdefgh", "1", "1111", "22222",
                                             "6412233", "2019-01-01", "2019-01-01T12:12:12",
                                             "15:10:00", "false", "1.2", "1.0", "11:12:12");
        List<String> fields4 = Arrays.asList("64", "65", "aba", "abcdefgh", "1", "1111", "22222",
                                             "6412233", "2019-01-01", "2019-01-01T12:12:12",
                                             "15:10:00", "false", "1.2", "1.0", "11:12:12");
        List<String> fields5 = Arrays.asList("65", "66", "aba", "abcdefgh", "1", "1111", "22222",
                                             "6412233", "2019-01-01", "2019-01-01T12:12:12",
                                             "15:10:00", "false", "1.2", "1.0", "11:12:12");
        List<String> fields6 = Arrays.asList("66", "67", "aba", "abcdefgh", "1", "1111", "22222",
                                             "6412233", "2019-01-01", "2019-01-01T12:12:12",
                                             "15:10:00", "false", "1.2", "1.0", "11:12:12");
        List<String> fields7 = Arrays.asList("67", "68", "李四", "abcdefgh", "1", "1111", "22222",
                                             "6412233", "2019-01-01", "2019-01-01T12:12:12",
                                             "15:10:00", "true", "1.2", "1.0", "11:12:12");
        List<String> fields8 = Arrays.asList("68", "61", "aba", "张三", "1", "1111", "22222",
                                             "6412233", "2019-01-01", "2019-01-01T12:12:12",
                                             "15:10:00", "true", "1.2", "1.0", "11:12:12");
        friend.add(fields1);
        friend.add(fields2);
        friend.add(fields3);
        friend.add(fields4);
        friend.add(fields5);
        friend.add(fields6);
        friend.add(fields7);
        friend.add(fields8);
        DataStream<List<String>> friendSource = env.fromCollection(friend);
        return friendSource;
    }

    /**
     * sink Nebula Graph
     */
    public static void sinkEdgeData(StreamExecutionEnvironment env,
                                    DataStream<List<String>> playerSource) {
        ConnectionOptions connectionOptions = getConnectionOptions();
        SinkEdgeOptions sinkEdgeOptions =
                SinkEdgeOptions.builder()
                        .withGraphName("flinkSink")
                        .withEdgeType("friend")
                        .withFlinkSrcPkFields(Arrays.asList("c0"))
                        .withNebulaSrcPks(Arrays.asList("col1"))
                        .withFlinkDstPkFields(Arrays.asList("c1"))
                        .withNebulaDstPks(Arrays.asList("col1"))
                        .withFlinkFields(Arrays.asList("c2", "c3", "c4", "c5", "c6", "c7", "c8",
                                                       "c9", "c10", "c11", "c12", "c13", "c14"))
                        .withNebulaFields(Arrays.asList("col1", "col2", "col3", "col4", "col5",
                                                        "col6", "col7", "col8", "col9", "col10",
                                                        "col11", "col12", "col13"))
                        .withWriteMode(WriteModeEnum.INSERTIGNORE)
                        .withBatchSize(10)
                        .build();

        NebulaEdgeBatchOutputFormat outputFormat = new NebulaEdgeBatchOutputFormat(
                connectionOptions, sinkEdgeOptions);
        NebulaSinkFunction<Row> nebulaSinkFunction = new NebulaSinkFunction<>(outputFormat);
        DataStream<Row> dataStream = playerSource.map(row -> {
            org.apache.flink.types.Row record = Row.withNames();
            for (int i = 0; i < row.size(); i++) {
                record.setField("c" + i, row.get(i));
            }
            return record;
        });
        dataStream.addSink(nebulaSinkFunction);
        try {
            env.execute("Write Nebula Edge");
        } catch (Exception e) {
            LOG.error("error when write Nebula Graph Edge, ", e);
            System.exit(-1);
        }
    }

    /**
     * sink Nebula Graph with UPDATE mode
     */
    public static void updateEdgeData(StreamExecutionEnvironment env,
                                      DataStream<List<String>> playerSource) {
        ConnectionOptions connectionOptions = getConnectionOptions();
        SinkEdgeOptions sinkEdgeOptions =
                SinkEdgeOptions.builder()
                        .withGraphName("flinkSink")
                        .withEdgeType("friend")
                        .withFlinkSrcPkFields(Arrays.asList("c0"))
                        .withNebulaSrcPks(Arrays.asList("col1"))
                        .withFlinkDstPkFields(Arrays.asList("c1"))
                        .withNebulaDstPks(Arrays.asList("col1"))
                        .withFlinkFields(Arrays.asList("c2", "c3", "c4", "c5", "c6", "c7", "c8",
                                                       "c9", "c10", "c11", "c12", "c13", "c14"))
                        .withNebulaFields(Arrays.asList("col1", "col2", "col3", "col4", "col5",
                                                        "col6", "col7", "col8", "col9", "col10",
                                                        "col11", "col12", "col13"))
                        .withWriteMode(WriteModeEnum.UPDATE)
                        .withBatchSize(2)
                        .build();

        NebulaEdgeBatchOutputFormat outputFormat = new NebulaEdgeBatchOutputFormat(
                connectionOptions, sinkEdgeOptions);
        NebulaSinkFunction<Row> nebulaSinkFunction = new NebulaSinkFunction<>(outputFormat);
        DataStream<Row> dataStream = playerSource.map(row -> {
            org.apache.flink.types.Row record = Row.withNames();
            for (int i = 0; i < row.size(); i++) {
                record.setField("c" + i, row.get(i));
            }
            return record;
        });
        dataStream.addSink(nebulaSinkFunction);
        try {
            env.execute("Update Nebula Edge");
        } catch (Exception e) {
            LOG.error("error when update Nebula Graph Edge, ", e);
            System.exit(-1);
        }
    }


    /**
     * sink Nebula Graph with DELETE mode
     */
    public static void deleteEdgeData(StreamExecutionEnvironment env,
                                      DataStream<List<String>> playerSource) {
        ConnectionOptions connectionOptions = getConnectionOptions();
        SinkEdgeOptions sinkEdgeOptions =
                SinkEdgeOptions.builder()
                        .withGraphName("flinkSink")
                        .withEdgeType("friend")
                        .withFlinkSrcPkFields(Arrays.asList("c0"))
                        .withNebulaSrcPks(Arrays.asList("col1"))
                        .withFlinkDstPkFields(Arrays.asList("c1"))
                        .withNebulaDstPks(Arrays.asList("col1"))
                        .withFlinkFields(Arrays.asList("c2", "c3", "c4", "c5", "c6", "c7", "c8",
                                                       "c9", "c10", "c11", "c12", "c13", "c14"))
                        .withNebulaFields(Arrays.asList("col1", "col2", "col3", "col4", "col5",
                                                        "col6", "col7", "col8", "col9", "col10",
                                                        "col11", "col12", "col13"))
                        .withWriteMode(WriteModeEnum.DELETE)
                        .withBatchSize(2)
                        .build();

        NebulaEdgeBatchOutputFormat outputFormat = new NebulaEdgeBatchOutputFormat(
                connectionOptions, sinkEdgeOptions);
        NebulaSinkFunction<Row> nebulaSinkFunction = new NebulaSinkFunction<>(outputFormat);
        DataStream<Row> dataStream = playerSource.map(row -> {
            org.apache.flink.types.Row record = Row.withNames();
            for (int i = 0; i < row.size(); i++) {
                record.setField("c" + i, row.get(i));
            }
            return record;
        });
        dataStream.addSink(nebulaSinkFunction);
        try {
            env.execute("Delete Nebula Edge");
        } catch (Exception e) {
            LOG.error("error when delete Nebula Graph Edge, ", e);
            System.exit(-1);
        }
    }
}
