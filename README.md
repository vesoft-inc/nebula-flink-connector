# nebula-flink-connector
Flink Connector for Nebula Graph


![](https://img.shields.io/badge/language-java-orange.svg)
[![GitHub stars](https://img.shields.io/github/stars/vesoft-inc/nebula-flink-connector.svg?color=brightgreen)](https://GitHub.com/vesoft-inc/nebula-flink-connector/stargazers/)
[![GitHub fork](https://img.shields.io/github/forks/vesoft-inc/nebula-flink-connector.svg?color=brightgreen)](https://GitHub.com/vesoft-inc/nebula-flink-connector/forks/)

Nebula-Flink-Connector 2.0/3.0 is a connector that helps Flink users to easily access Nebula Graph 2.0/3.0. If you want to access Nebula Graph 1.x with Flink, please refer to [Nebula-Flink-Connector 1.0](https://github.com/vesoft-inc/nebula-java/tree/v1.0/tools/nebula-flink).

## Quick start

### Prerequisites

To use Nebula Flink Connector, do a check of these:

- Java 8 or a higher version is installed.
- Nebula Graph is deployed. For more information, see [Deployment and installation of Nebula Graph](https://docs.nebula-graph.io/2.0/4.deployment-and-installation/1.resource-preparations/ "Click to go to Nebula Graph website").

### Use in Maven
Add the dependency to your pom.xml.

```
<dependency>
    <groupId>com.vesoft</groupId>
    <artifactId>nebula-flink-connector</artifactId>
    <version>3.0-SNAPSHOT</version>
</dependency>
```


## Example

To write data into NebulaGraph using Flink.
```
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
NebulaClientOptions nebulaClientOptions = new NebulaClientOptions.NebulaClientOptionsBuilder()
                .setGraphAddress("127.0.0.1:9669")
                .setMetaAddress("127.0.0.1:9559")
                .build();
NebulaGraphConnectionProvider graphConnectionProvider = new NebulaGraphConnectionProvider(nebulaClientOptions);
NebulaMetaConnectionProvider metaConnectionProvider = new NebulaMetaConnectionProvider(nebulaClientOptions);

VertexExecutionOptions executionOptions = new VertexExecutionOptions.ExecutionOptionBuilder()
                .setGraphSpace("flinkSink")
                .setTag("player")
                .setIdIndex(0)
                .setFields(Arrays.asList("name", "age"))
                .setPositions(Arrays.asList(1, 2))
                .setBatchSize(2)
                .build();

NebulaVertexBatchOutputFormat outputFormat = new NebulaVertexBatchOutputFormat(
                graphConnectionProvider, metaConnectionProvider, executionOptions);
NebulaSinkFunction<Row> nebulaSinkFunction = new NebulaSinkFunction<>(outputFormat);
DataStream<Row> dataStream = playerSource.map(row -> {
            Row record = new org.apache.flink.types.Row(row.size());
            for (int i = 0; i < row.size(); i++) {
                record.setField(i, row.get(i));
            }
            return record;
        });
dataStream.addSink(nebulaSinkFunction);
env.execute("write nebula")
```

To read data from NebulaGraph using Flink.
```
        NebulaClientOptions nebulaClientOptions = new NebulaClientOptions.NebulaClientOptionsBuilder()
                .setMetaAddress("127.0.0.1:9559")
                .build();
        storageConnectionProvider = new NebulaStorageConnectionProvider(nebulaClientOptions);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        VertexExecutionOptions vertexExecutionOptions = new VertexExecutionOptions.ExecutionOptionBuilder()
                .setGraphSpace("flinkSource")
                .setTag("person")
                .setNoColumn(false)
                .setFields(Arrays.asList())
                .setLimit(100)
                .build();
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
```

To operate Schema and data using Flink SQL.

1. create graph space
```
        NebulaCatalog nebulaCatalog = NebulaCatalogUtils.createNebulaCatalog(
                "NebulaCatalog",
                "default",
                "root",
                "nebula",
                "127.0.0.1:9559",
                "127.0.0.1:9669");

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        tableEnv.registerCatalog(CATALOG_NAME, nebulaCatalog);
        tableEnv.useCatalog(CATALOG_NAME);

        String createDataBase = "CREATE DATABASE IF NOT EXISTS `db1`"
                + " COMMENT 'space 1'"
                + " WITH ("
                + " 'partition_num' = '100',"
                + " 'replica_factor' = '3',"
                + " 'vid_type' = 'FIXED_STRING(10)'"
                + ")";
        tableEnv.executeSql(createDataBase);
```
2. create tag
```
        tableEnvironment.executeSql("CREATE TABLE `person` ("
                + " vid BIGINT,"
                + " col1 STRING,"
                + " col2 STRING,"
                + " col3 BIGINT,"
                + " col4 BIGINT,"
                + " col5 BIGINT,"
                + " col6 BIGINT,"
                + " col7 DATE,"
                + " col8 TIMESTAMP,"
                + " col9 BIGINT,"
                + " col10 BOOLEAN,"
                + " col11 DOUBLE,"
                + " col12 DOUBLE,"
                + " col13 TIME,"
                + " col14 STRING"
                + ") WITH ("
                + " 'connector' = 'nebula',"
                + " 'meta-address' = '127.0.0.1:9559',"
                + " 'graph-address' = '127.0.0.1:9669',"
                + " 'username' = 'root',"
                + " 'password' = 'nebula',"
                + " 'data-type' = 'vertex',"
                + " 'graph-space' = 'flink_test',"
                + " 'label-name' = 'person'"
                + ")"
        );
```
3. create edge
```
        tableEnvironment.executeSql("CREATE TABLE `friend` ("
                + " sid BIGINT,"
                + " did BIGINT,"
                + " rid BIGINT,"
                + " col1 STRING,"
                + " col2 STRING,"
                + " col3 BIGINT,"
                + " col4 BIGINT,"
                + " col5 BIGINT,"
                + " col6 BIGINT,"
                + " col7 DATE,"
                + " col8 TIMESTAMP,"
                + " col9 BIGINT,"
                + " col10 BOOLEAN,"
                + " col11 DOUBLE,"
                + " col12 DOUBLE,"
                + " col13 TIME,"
                + " col14 STRING"
                + ") WITH ("
                + " 'connector' = 'nebula',"
                + " 'meta-address' = '127.0.0.1:9559',"
                + " 'graph-address' = '127.0.0.1:9669',"
                + " 'username' = 'root',"
                + " 'password' = 'nebula',"
                + " 'graph-space' = 'flink_test',"
                + " 'label-name' = 'friend',"
                + " 'data-type'='edge',"
                + " 'src-id-index'='0',"
                + " 'dst-id-index'='1',"
                + " 'rank-id-index'='2'"
                + ")"
        );
```
4. query edge data and insert into another edge type
```
        Table table = tableEnvironment.sqlQuery("SELECT * FROM `friend`");
        table.executeInsert("`friend_sink`").await();
```

## Version match

There are the version correspondence between Nebula Flink Connector and Nebula:

| Nebula Flink Connector Version | Nebula Version |
|:------------------------------:|:--------------:|
|             2.0.0              |  2.0.0, 2.0.1  |
|             2.5.0              |  2.5.0, 2.5.1  |
|             2.6.0              |  2.6.0, 2.6.1  |
|             2.6.1              |  2.6.0, 2.6.1  |
|             3.0.0              |     3.x.x      | 
|             3.3.0              |     3.x.x      | 
|             3.5.0              |     3.x.x      | 
|          3.0-SNAPSHOT          |    nightly     |

## Note
Flink version requirements: 1.11.x
