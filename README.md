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

To write data into Nebula Graph using Flink.
```
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
NebulaClientOptions nebulaClientOptions = new NebulaClientOptions.NebulaClientOptionsBuilder()
                .setGraphAddress("127.0.0.1:9669")
                .setMetaAddress("127.0.0.1:9559")
                .build();
NebulaGraphConnectionProvider graphConnectionProvider = new NebulaGraphConnectionProvider(nebulaClientOptions);
NebulaMetaConnectionProvider metaConnectionProvider = new NebulaMetaConnectionProvider(nebulaClientOptions);

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
            Row record = new org.apache.flink.types.Row(row.size());
            for (int i = 0; i < row.size(); i++) {
                record.setField(i, row.get(i));
            }
            return record;
        });
dataStream.addSink(nebulaSinkFunction);
env.execute("write nebula")
```
## Version match

There are the version correspondence between Nebula Flink Connector and Nebula:

| Nebula Flink Connector Version | Nebula Version |
|:-----------------------:|:--------------:|
|       2.0.0             |  2.0.0, 2.0.1  |
|       2.5.0             |  2.5.0, 2.5.1  |
|       2.6.0             |     2.6.0      |
|     3.0-SNAPSHOT        |     nightly    |
