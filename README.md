# nebula-flink-connector
Flink Connector for Nebula Graph 5


![](https://img.shields.io/badge/language-java-orange.svg)
[![GitHub stars](https://img.shields.io/github/stars/vesoft-inc/nebula-flink-connector.svg?color=brightgreen)](https://GitHub.com/vesoft-inc/nebula-flink-connector/stargazers/)
[![GitHub fork](https://img.shields.io/github/forks/vesoft-inc/nebula-flink-connector.svg?color=brightgreen)](https://GitHub.com/vesoft-inc/nebula-flink-connector/forks/)

Nebula-Flink-Connector 5 is a connector that helps Flink users to easily access Nebula Graph 5. 
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
    <version>5.0-SNAPSHOT</version>
</dependency>
```


## Example

To write data into Nebula Graph Node using Flink.
```
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
ConnectionOptions connectionOptions = ConnectionOptions
                .builder()
                .withGraphAddress("127.0.0.1:9669")
                .withUser("root")
                .withPassword("NebulaGraph01")
                .build();
SinkNodeOptions sinkNodeOptions = SinkNodeOptions.builder()
                        .withGraphName("flinkSink")
                        .withNodeType("person")
                        .withFlinkFields(Arrays.asList("c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12"))
                        .withNebulaFields(Arrays.asList("col1", "col2", "col3", "col4", "col5", "col6","col7", "col8", "col9", "col10", "col11", "col12", "col13"))
                        .withWriteMode(WriteModeEnum.INSERTREPLACE)
                        .withBatchSize(2)
                        .build();


NebulaNodeBatchOutputFormat outputFormat       = new NebulaNodeBatchOutputFormat(connectionOptions, sinkNodeOptions);
NebulaSinkFunction<Row>     nebulaSinkFunction = new NebulaSinkFunction<>(outputFormat);
DataStream<Row> dataStream = playerSource.map(row -> {
            org.apache.flink.types.Row record = Row.withNames();
            for (int i = 0; i < row.size(); i++) {
                record.setField("c" + i, row.get(i));
            }
            return record;
        });
dataStream.print().name("print");
dataStream.addSink(nebulaSinkFunction);
env.execute("write nebula");
```

To write data into NebulaGraph Edge using Flink.
```agsl
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
ConnectionOptions connectionOptions = ConnectionOptions
                .builder()
                .withGraphAddress("127.0.0.1:9669")
                .withUser("root")
                .withPassword("NebulaGraph01")
                .build();
SinkEdgeOptions sinkEdgeOptions = SinkEdgeOptions.builder()
                        .withGraphName("flinkSink")
                        .withEdgeType("friend")
                        .withFlinkSrcPkFields(Arrays.asList("c0"))
                        .withNebulaSrcPks(Arrays.asList("col1"))
                        .withFlinkDstPkFields(Arrays.asList("c1"))
                        .withNebulaDstPks(Arrays.asList("col1"))
                        .withFlinkFields(Arrays.asList("c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13", "c14"))
                        .withNebulaFields(Arrays.asList("col1", "col2", "col3", "col4", "col5", "col6", "col7", "col8", "col9", "col10", "col11", "col12", "col13"))
                        .withWriteMode(WriteModeEnum.INSERTIGNORE)
                        .withBatchSize(2)
                        .build();

NebulaEdgeBatchOutputFormat outputFormat = new NebulaEdgeBatchOutputFormat(connectionOptions, sinkEdgeOptions);
NebulaSinkFunction<Row> nebulaSinkFunction = new NebulaSinkFunction<>(outputFormat);
DataStream<Row> dataStream = playerSource.map(row -> {
            org.apache.flink.types.Row record = Row.withNames();
            for (int i = 0; i < row.size(); i++) {
                record.setField("c" + i, row.get(i));
            }
            return record;
        });
dataStream.addSink(nebulaSinkFunction);
env.execute("Write Nebula Edge");
```

To read Node data to Flink Row from NebulaGraph using Flink.
```agsl
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
ConnectionOptions connectionOptions = ConnectionOptions
                .builder()
                .withGraphAddress("127.0.0.1:9669")
                .withUser("root")
                .withPassword("NebulaGraph01")
                .build();
SourceExecutionOptions nodeExecutionOptions = SourceNodeOptions.builder()
                .withGraphName("flinkSource")
                .withNodeType("person")
                .withReturnCols(null)
                .withBatchSize(10)
                .build();

NebulaInputRowFormat inputRowFormat = new NebulaInputRowFormat(connectionOptions, nodeExecutionOptions);
DataSource<Row> rowDataSource = env.createInput(inputRowFormat);

System.out.println("rowDataSource count: " + rowDataSource.count());
```
To read Edge data to Flink Row from NebulaGraph using Flink.
```agsl
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
ConnectionOptions connectionOptions = ConnectionOptions
                .builder()
                .withGraphAddress("127.0.0.1:9669")
                .withUser("root")
                .withPassword("NebulaGraph01")
                .build();
SourceExecutionOptions edgeExecutionOptions = SourceEdgeOptions.builder()
                .withGraphName("flinkSource")
                .withEdgeType("friend")
                .withReturnCols(null)
                .withBatchSize(10)
                .build();

NebulaInputRowFormat inputRowFormat = new NebulaInputRowFormat(connectionOptions, edgeExecutionOptions);
DataSource<Row> rowDataSource = env.createInput(inputRowFormat);

System.out.println("rowDataSource count: " + rowDataSource.count());
```

for more examples, see https://github.com/vesoft-inc/nebula-flink-connector/tree/master/example/src/main/java/org/apache/flink

## Version match

There are the version correspondence between Nebula Flink Connector and Nebula:

| Nebula Flink Connector Version | Nebula Version |
|:------------------------------:|:--------------:|
|             2.0.0              |  2.0.0, 2.0.1  |
|             2.5.0              |  2.5.0, 2.5.1  |
|             2.6.0              |  2.6.0, 2.6.1  |
|             2.6.1              |  2.6.0, 2.6.1  |
|             3.0.0              |      3.x       | 
|             3.5.0              |      3.x       |
|          5.0-SNAPSHOT          |      5.x       |


## Note
Flink version requirement: 1.14.x

Scala version requirement: 2.11

JDK version requirement: 1.8
