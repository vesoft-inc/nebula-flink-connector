/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
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
import org.apache.flink.connector.nebula.utils.SSLSignType;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * make sure your environment has creates space, and data has been insert into this space.
 * Space schema:
 *
 * <p>"CREATE SPACE `flinkSource` (partition_num = 100, replica_factor = 3, charset = utf8,
 * collate = utf8_bin, vid_type = INT64, atomic_edge = false)"
 *
 * <p>"USE `flinkSource`"
 *
 * <p>"CREATE TAG IF NOT EXISTS person(col1 string, col2 fixed_string(8), col3 int8, col4 int16,
 * col5 int32, col6 int64, col7 date, col8 datetime, col9 timestamp, col10 bool, col11 double,
 * col12 float, col13 time, col14 geography);"
 *
 * <p>"CREATE EDGE IF NOT EXISTS friend(col1 string, col2 fixed_string(8), col3 int8, col4 int16,
 * col5 int32, col6 int64, col7 date, col8 datetime, col9 timestamp, col10 bool, col11 double,
 * col12 float, col13 time, col14 geography);"
 */
public class FlinkConnectorSourceExample {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkConnectorSourceExample.class);

    private static NebulaStorageConnectionProvider storageConnectionProvider;
    private static NebulaStorageConnectionProvider storageConnectionProviderCaSSL;
    private static NebulaStorageConnectionProvider storageConnectionProviderSelfSSL;
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

        NebulaClientOptions nebulaClientOptionsWithCaSSL =
                new NebulaClientOptions.NebulaClientOptionsBuilder()
                        .setMetaAddress("127.0.0.1:9559")
                        .setEnableMetaSSL(true)
                        .setEnableStorageSSL(true)
                        .setSSLSignType(SSLSignType.CA)
                        .setCaSignParam("example/src/main/resources/ssl/casigned.pem",
                                "example/src/main/resources/ssl/casigned.crt",
                                "example/src/main/resources/ssl/casigned.key")
                        .build();
        storageConnectionProviderCaSSL =
                new NebulaStorageConnectionProvider(nebulaClientOptionsWithCaSSL);

        NebulaClientOptions nebulaClientOptionsWithSelfSSL =
                new NebulaClientOptions.NebulaClientOptionsBuilder()
                        .setMetaAddress("127.0.0.1:9559")
                        .setEnableMetaSSL(true)
                        .setEnableStorageSSL(true)
                        .setSSLSignType(SSLSignType.SELF)
                        .setSelfSignParam("example/src/main/resources/ssl/selfsigned.pem",
                                "example/src/main/resources/ssl/selfsigned.key",
                                "vesoft")
                        .build();
        storageConnectionProviderSelfSSL =
                new NebulaStorageConnectionProvider(nebulaClientOptionsWithSelfSSL);

        // read no property
        vertexExecutionOptions = new VertexExecutionOptions.ExecutionOptionBuilder()
                .setGraphSpace("flinkSource")
                .setTag("person")
                .setNoColumn(false)
                .setFields(Arrays.asList())
                .setLimit(100)
                .build();

        // read specific properties
        // if you want to read all properties, config: setFields(Arrays.asList())
        edgeExecutionOptions = new EdgeExecutionOptions.ExecutionOptionBuilder()
                .setGraphSpace("flinkSource")
                .setEdge("friend")
                //.setFields(Arrays.asList("col1", "col2","col3"))
                .setFields(Arrays.asList())
                .setLimit(100)
                .build();

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
            Row record = new Row(15);
            record.setField(0, values.get(0).asLong());
            record.setField(1, values.get(1).asString());
            record.setField(2, values.get(2).asString());
            record.setField(3, values.get(3).asLong());
            record.setField(4, values.get(4).asLong());
            record.setField(5, values.get(5).asLong());
            record.setField(6, values.get(6).asLong());
            record.setField(7, values.get(7).asDate());
            record.setField(8, values.get(8).asDateTime().getUTCDateTimeStr());
            record.setField(9, values.get(9).asLong());
            record.setField(10, values.get(10).asBoolean());
            record.setField(11, values.get(11).asDouble());
            record.setField(12, values.get(12).asDouble());
            record.setField(13, values.get(13).asTime().getUTCTimeStr());
            record.setField(14, values.get(14).asGeography());
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
        NebulaSourceFunction sourceFunction = new NebulaSourceFunction(storageConnectionProvider)
                .setExecutionOptions(edgeExecutionOptions);
        DataStreamSource<BaseTableRow> dataStreamSource = env.addSource(sourceFunction);

        dataStreamSource.map(row -> {
            List<ValueWrapper> values = row.getValues();
            Row record = new Row(17);
            record.setField(0, values.get(0).asLong());
            record.setField(1, values.get(1).asLong());
            record.setField(2, values.get(2).asLong());
            record.setField(3, values.get(3).asString());
            record.setField(4, values.get(4).asString());
            record.setField(5, values.get(5).asLong());
            record.setField(6, values.get(6).asLong());
            record.setField(7, values.get(7).asLong());
            record.setField(8, values.get(8).asLong());
            record.setField(9, values.get(9).asDate());
            record.setField(10, values.get(10).asDateTime().getUTCDateTimeStr());
            record.setField(11, values.get(11).asLong());
            record.setField(12, values.get(12).asBoolean());
            record.setField(13, values.get(13).asDouble());
            record.setField(14, values.get(14).asDouble());
            record.setField(15, values.get(15).asTime().getUTCTimeStr());
            record.setField(16, values.get(16).asGeography());
            return record;
        }).print();
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
