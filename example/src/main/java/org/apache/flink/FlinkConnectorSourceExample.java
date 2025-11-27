/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink;

import com.vesoft.nebula.driver.graph.data.ResultSet;
import com.vesoft.nebula.driver.graph.data.ValueWrapper;
import com.vesoft.nebula.driver.graph.net.NebulaClient;
import com.vesoft.nebula.driver.graph.scan.TableRow;
import java.time.format.DateTimeFormatter;
import java.util.List;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.connector.nebula.options.ConnectionOptions;
import org.apache.flink.connector.nebula.options.SourceEdgeOptions;
import org.apache.flink.connector.nebula.options.SourceExecutionOptions;
import org.apache.flink.connector.nebula.options.SourceNodeOptions;
import org.apache.flink.connector.nebula.source.NebulaInputRowFormat;
import org.apache.flink.connector.nebula.source.NebulaInputTableRowFormat;
import org.apache.flink.connector.nebula.source.NebulaSourceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * make sure your environment has creates graph, and data has been insert into this graph.
 */
public class FlinkConnectorSourceExample {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkConnectorSourceExample.class);

    /**
     * Read streaming Nebula data only supports TableRow format.
     * Read batch Nebula data supports nebula's TableRow format and flink's Row format.
     */
    public static void main(String[] args) throws Exception {
        prepareGraphData();
        nebulaNodeStreamSource();
        nebulaNodeBatchSource();

        nebulaEdgeStreamSource();
        nebulaEdgeBatchSource();

        System.exit(0);
    }


    private static void prepareGraphData() {
        String graphType = "CREATE GRAPH TYPE IF NOT EXISTS flinkSourceType AS{\n"
                + "NODE TYPE person(LABEL person{col1 string primary key, col2 string, col3 int8,"
                + " col4 int16,col5 int32, col6 int64, col7 date, col8 local datetime, "
                + "col9 local time, col10 bool, col11 double, col12 float, col13 zoned time}),\n"
                + "EDGE TYPE friend(person)-[LABEL friend{col1 string, col2 string, col3 int8, "
                + "col4 int16, col5 int32, col6 int64, col7 date, col8 local datetime, "
                + "col9 local time, col10 bool, col11 double,col12 float, col13 zoned time}]"
                + "->(person)\n"
                + " }";
        String       graph  = "CREATE GRAPH IF NOT EXISTS flinkSource TYPED flinkSourceType";
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

            client.execute("SESSION SET zoned_time_format=\"%H:%M:%S\"");
            String insertNode;
            for (int i = 0; i < 100; i++) {
                insertNode = String.format(
                        "TABLE t{c0,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12} = \n"
                                + "(\"%d\",\"aba\",1,1111,22222,6412233,date(\"2019-01-01\"),"
                                + "local_datetime(\"2019-01-01T12:12:12\"),local_time(\"10:10:10\")"
                                + ",false,1.2,1.0,zoned_time(\"11:12:12\")) \n"
                                + "USE `flinkSource` \n"
                                + "FOR r IN t \n"
                                + "INSERT OR REPLACE (@`person`{`col1`:r.c0,`col2`:r.c1,`col3`:"
                                + "r.c2,`col4`:r.c3,`col5`:r.c4,`col6`:r.c5,`col7`:r.c6,`col8`:"
                                + "r.c7,`col9`:r.c8,`col10`:r.c9,`col11`:r.c10,`col12`:r.c11,"
                                + "`col13`:r.c12})", i);
                res = client.execute(insertNode);
                if (!res.isSucceeded()) {
                    LOG.error("insert node failed:" + res.getErrorMessage());
                    System.exit(1);
                }
            }
            String insertEdge;
            for (int i = 0; i < 100; i++) {
                for (int j = 0; j < 10; j++) {
                    insertEdge = String.format(
                            "TABLE t{src_0,dst_0,c0,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12} = \n"
                                    + "(\"%d\",\"%d\",\"aba\",\"abcdefgh\",1,1111,22222,6412233,"
                                    + "date(\"2019-01-01\"),local_datetime(\"2019-01-01T12:12:12\")"
                                    + ",local_time(\"15:10:00\"),false,1.2,1.0,"
                                    + "zoned_time(\"11:12:12\")) \n"
                                    + "USE `flinkSource` \n"
                                    + "FOR r IN t \n"
                                    + "OPTIONAL MATCH (n_src@`person`) WHERE n_src.`col1`=r.src_0 "
                                    + "OPTIONAL MATCH (n_dst@`person`) WHERE n_dst.`col1`=r.dst_0\n"
                                    + "INSERT OR IGNORE (n_src)-[@`friend`{`col1`:r.c0,`col2`:r.c1,"
                                    + "`col3`:r.c2,`col4`:r.c3,`col5`:r.c4,`col6`:r.c5,`col7`:r.c6,"
                                    + "`col8`:r.c7,`col9`:r.c8,`col10`:r.c9,`col11`:r.c10,`col12`:"
                                    + "r.c11,`col13`:r.c12}]->(n_dst)", i, j);
                    res = client.execute(insertEdge);
                    if (!res.isSucceeded()) {
                        LOG.error("insert edge failed:" + res.getErrorMessage());
                        System.exit(1);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        } finally {
            if (client != null) {
                client.close();
            }
        }
        LOG.info("prepare source data finished!");
    }

    private static ConnectionOptions getConnectionOptions() {
        ConnectionOptions connectionOptions = ConnectionOptions
                .builder()
                .withGraphAddress("192.168.8.6:3820")
                .withUser("root")
                .withPassword("NebulaGraph01")
                .build();
        return connectionOptions;
    }

    private static SourceExecutionOptions getNodeExecutionOptions() {
        SourceExecutionOptions nodeExecutionOptions = SourceNodeOptions.builder()
                .withGraphName("flinkSource")
                .withNodeType("person")
                .withReturnCols(null)
                .withBatchSize(10)
                .build();
        return nodeExecutionOptions;
    }

    private static SourceExecutionOptions getEdgeExecutionOptions() {
        SourceExecutionOptions edgeExecutionOptions = SourceEdgeOptions.builder()
                .withGraphName("flinkSource")
                .withEdgeType("friend")
                .withReturnCols(null)
                .withBatchSize(10)
                .build();
        return edgeExecutionOptions;
    }


    /**
     * read Nebula Graph Node as stream data source
     */
    public static void nebulaNodeStreamSource() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        NebulaSourceFunction sourceFunction = new NebulaSourceFunction(getConnectionOptions(),
                                                                       getNodeExecutionOptions());
        DataStreamSource<TableRow> dataStreamSource = env.addSource(sourceFunction);

        dataStreamSource.map(row -> {
            List<ValueWrapper> values = row.getValues();
            Row                record = new Row(13);
            record.setField(0, values.get(0).asString());
            record.setField(1, values.get(1).asString());
            record.setField(2, values.get(2).asInt());
            record.setField(3, values.get(3).asInt());
            record.setField(4, values.get(4).asInt());
            record.setField(5, values.get(5).asLong());
            record.setField(6, values.get(6).asDate());
            record.setField(7, values.get(7).asLocalDateTime());
            record.setField(8, values.get(8).asLocalTime());
            record.setField(9, values.get(9).asBoolean());
            record.setField(10, values.get(10).asDouble());
            record.setField(11, values.get(11).asFloat());
            record.setField(12, values.get(12)
                    .asZonedTime()
                    .format(DateTimeFormatter.ofPattern("HH:mm:ss")));
            return record;
        }).print();
        env.execute("NebulaStreamSource");
    }


    /**
     * read Nebula Graph edge as stream data source
     */
    public static void nebulaEdgeStreamSource() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        // get Nebula Graph data in BaseTableRow format
        NebulaSourceFunction sourceFunction = new NebulaSourceFunction(getConnectionOptions(),
                                                                       getEdgeExecutionOptions());
        DataStreamSource<TableRow> dataStreamSource = env.addSource(sourceFunction);

        dataStreamSource.map(row -> {
            List<ValueWrapper> values = row.getValues();
            Row                record = new Row(15);
            record.setField(0, values.get(0).asString());
            record.setField(1, values.get(1).asString());
            record.setField(2, values.get(2).asString());
            record.setField(3, values.get(3).asString());
            record.setField(4, values.get(4).asInt());
            record.setField(5, values.get(5).asInt());
            record.setField(6, values.get(6).asInt());
            record.setField(7, values.get(7).asLong());
            record.setField(8, values.get(8).asDate());
            record.setField(9, values.get(9).asLocalDateTime());
            record.setField(10, values.get(10).asLocalTime());
            record.setField(11, values.get(11).asBoolean());
            record.setField(12, values.get(12).asDouble());
            record.setField(13, values.get(13).asFloat());
            record.setField(14, values.get(14)
                    .asZonedTime()
                    .format(DateTimeFormatter.ofPattern("HH:mm:ss")));
            return record;
        }).print();
        env.execute("NebulaStreamSource");
    }

    /**
     * read Nebula Graph vertex as batch data source
     */
    public static void nebulaNodeBatchSource() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // get Nebula node data in flink Row format
        NebulaInputRowFormat inputRowFormat = new NebulaInputRowFormat(getConnectionOptions(),
                                                                       getNodeExecutionOptions());
        DataSource<Row> rowDataSource = env.createInput(inputRowFormat);
        rowDataSource.print();
        System.out.println("rowDataSource count: " + rowDataSource.count());

        // get Nebula vertex data in nebula TableRow format
        NebulaInputTableRowFormat inputFormat =
                new NebulaInputTableRowFormat(getConnectionOptions(),
                                              getNodeExecutionOptions());
        DataSource<TableRow> dataSource = env.createInput(inputFormat);
        dataSource.map(row -> {
            List<ValueWrapper> values = row.getValues();
            Row                record = new Row(13);
            record.setField(0, values.get(0).asString());
            record.setField(1, values.get(1).asString());
            record.setField(2, values.get(2).asInt());
            record.setField(3, values.get(3).asInt());
            record.setField(4, values.get(4).asInt());
            record.setField(5, values.get(5).asLong());
            record.setField(6, values.get(6).asDate());
            record.setField(7, values.get(7).asLocalDateTime());
            record.setField(8, values.get(8).asLocalTime());
            record.setField(9, values.get(9).asBoolean());
            record.setField(10, values.get(10).asDouble());
            record.setField(11, values.get(11).asFloat());
            record.setField(12, values.get(12)
                    .asZonedTime()
                    .format(DateTimeFormatter.ofPattern("HH:mm:ss")));
            return record;
        }).print();
        System.out.println("datasource count: " + dataSource.count());
    }

    /**
     * read Nebula Graph edge as batch data source
     */
    public static void nebulaEdgeBatchSource() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // get Nebula edge data in flink Row format
        NebulaInputRowFormat inputFormat = new NebulaInputRowFormat(getConnectionOptions(),
                                                                    getEdgeExecutionOptions());
        DataSource<Row> dataSourceRow = env.createInput(inputFormat);
        dataSourceRow.print();
        System.out.println("datasource count: " + dataSourceRow.count());

        // get Nebula edge data in Nebula TableRow format
        NebulaInputTableRowFormat inputTableFormat =
                new NebulaInputTableRowFormat(getConnectionOptions(),
                                              getEdgeExecutionOptions());
        DataSource<TableRow> dataSourceTableRow = env.createInput(inputTableFormat);
        dataSourceTableRow.map(row -> {
            List<ValueWrapper> values = row.getValues();
            Row                record = new Row(15);
            record.setField(0, values.get(0).asString());
            record.setField(1, values.get(1).asString());
            record.setField(2, values.get(2).asString());
            record.setField(3, values.get(3).asString());
            record.setField(4, values.get(4).asInt());
            record.setField(5, values.get(5).asInt());
            record.setField(6, values.get(6).asInt());
            record.setField(7, values.get(7).asLong());
            record.setField(8, values.get(8).asDate());
            record.setField(9, values.get(9).asLocalDateTime());
            record.setField(10, values.get(10).asLocalTime());
            record.setField(11, values.get(11).asBoolean());
            record.setField(12, values.get(12).asDouble());
            record.setField(13, values.get(13).asFloat());
            record.setField(14, values.get(14)
                    .asZonedTime()
                    .format(DateTimeFormatter.ofPattern("HH:mm:ss")));
            return record;
        }).print();
        System.out.println("datasource count: " + dataSourceTableRow.count());
    }
}
