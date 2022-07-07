/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.table;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.nebula.connection.NebulaClientOptions;
import org.apache.flink.connector.nebula.statement.EdgeExecutionOptions;
import org.apache.flink.connector.nebula.statement.ExecutionOptions;
import org.apache.flink.connector.nebula.statement.VertexExecutionOptions;
import org.apache.flink.connector.nebula.utils.DataTypeEnum;
import org.apache.flink.connector.nebula.utils.NebulaConstant;
import org.apache.flink.connector.nebula.utils.WriteModeEnum;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;


public class NebulaDynamicTableFactory implements DynamicTableSourceFactory,
        DynamicTableSinkFactory {
    public static final String IDENTIFIER = "nebula";

    public static final ConfigOption<String> METAADDRESS = ConfigOptions
            .key("meta-address")
            .stringType()
            .noDefaultValue()
            .withDescription("the nebula meta server address.");

    public static final ConfigOption<String> GRAPHADDRESS = ConfigOptions
            .key("graph-address")
            .stringType()
            .noDefaultValue()
            .withDescription("the nebula graph server address.");

    public static final ConfigOption<String> USERNAME = ConfigOptions
            .key("username")
            .stringType()
            .noDefaultValue()
            .withDescription("the nebula server name.");

    public static final ConfigOption<String> PASSWORD = ConfigOptions
            .key("password")
            .stringType()
            .noDefaultValue()
            .withDescription("the nebula server password.");

    public static final ConfigOption<String> GRAPH_SPACE = ConfigOptions
            .key("graph-space")
            .stringType()
            .noDefaultValue()
            .withDescription("the nebula graph space name.");

    public static final ConfigOption<String> LABEL_NAME = ConfigOptions
            .key("label-name")
            .stringType()
            .noDefaultValue()
            .withDescription("the nebula graph space label name.");

    public static final ConfigOption<Integer> CONNECT_TIMEOUT = ConfigOptions
            .key("connect-timeout")
            .intType()
            .defaultValue(NebulaConstant.DEFAULT_CONNECT_TIMEOUT_MS)
            .withDescription("the nebula connect timeout duration");

    public static final ConfigOption<Integer> CONNECT_RETRY = ConfigOptions
            .key("connect-retry")
            .intType()
            .defaultValue(NebulaConstant.DEFAULT_CONNECT_RETRY)
            .withDescription("the nebula connect retry times.");

    public static final ConfigOption<Integer> TIMEOUT = ConfigOptions
            .key("timeout")
            .intType()
            .defaultValue(NebulaConstant.DEFAULT_TIMEOUT_MS)
            .withDescription("the nebula execute timeout duration.");

    public static final ConfigOption<Integer> EXECUTE_RETRY = ConfigOptions
            .key("execute-retry")
            .intType()
            .defaultValue(NebulaConstant.DEFAULT_EXECUTION_RETRY)
            .withDescription("the nebula execute retry times.");

    public static final ConfigOption<String> WRITE_MODE = ConfigOptions
            .key("write-mode")
            .stringType()
            .defaultValue("INSERT")
            .withDescription("the nebula graph write mode.");

    public static final ConfigOption<Integer> SRC_INDEX = ConfigOptions
            .key("src-index")
            .intType()
            .defaultValue(NebulaConstant.DEFAULT_ROW_INFO_INDEX)
            .withDescription("the nebula edge src index.");

    public static final ConfigOption<Integer> DST_INDEX = ConfigOptions
            .key("dst-index")
            .intType()
            .defaultValue(NebulaConstant.DEFAULT_ROW_INFO_INDEX)
            .withDescription("the nebula edge dst index.");

    public static final ConfigOption<Integer> RANK_INDEX = ConfigOptions
            .key("rank-index")
            .intType()
            .defaultValue(NebulaConstant.DEFAULT_ROW_INFO_INDEX)
            .withDescription("the nebula edge rank index.");

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig readableConfig = helper.getOptions();
        helper.validate();
        validateConfigOptions(readableConfig);
        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        ExecutionOptions executionOptions =
                getExecutionOptions(readableConfig, physicalSchema, context);
        NebulaClientOptions nebulaClientOptions = getNebulaClientOptions(readableConfig);
        return new NebulaDynamicTableSource(nebulaClientOptions, executionOptions, physicalSchema);
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig readableConfig = helper.getOptions();
        helper.validate();
        validateConfigOptions(readableConfig);
        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        ExecutionOptions executionOptions =
                getExecutionOptions(readableConfig, physicalSchema, context);
        NebulaClientOptions nebulaClientOptions = getNebulaClientOptions(readableConfig);
        return new NebulaDynamicTableSink(nebulaClientOptions, executionOptions, physicalSchema);
    }

    private ExecutionOptions getExecutionOptions(ReadableConfig readableConfig,
                                                 TableSchema physicalSchema, Context context) {
        String[] fieldNames = physicalSchema.getFieldNames();
        List<String> fieldList = new ArrayList<>();
        List<Integer> positionList = new ArrayList<>();
        String objectName = context.getObjectIdentifier().getObjectName();
        String[] typeAndLabel = objectName.split(NebulaConstant.SPLIT_POINT);
        String type = typeAndLabel[0];
        WriteModeEnum writeMode = WriteModeEnum.chooseWriteMode(readableConfig.get(WRITE_MODE));

        ExecutionOptions executionOptions;
        if (DataTypeEnum.VERTEX.name().equals(type)) {
            for (int i = 1; i < fieldNames.length; i++) {
                fieldList.add(fieldNames[i]);
                positionList.add(i);
            }
            executionOptions = new VertexExecutionOptions.ExecutionOptionBuilder()
                    .setGraphSpace(readableConfig.get(GRAPH_SPACE))
                    .setTag(readableConfig.get(LABEL_NAME))
                    .setIdIndex(0)
                    .setFields(fieldList)
                    .setPositions(positionList)
                    .setWriteMode(writeMode)
                    .builder();
        } else {
            for (int i = 3; i < fieldNames.length; i++) {
                fieldList.add(fieldNames[i]);
                positionList.add(i);
            }
            executionOptions = new EdgeExecutionOptions.ExecutionOptionBuilder()
                    .setGraphSpace(readableConfig.get(GRAPH_SPACE))
                    .setEdge(readableConfig.get(LABEL_NAME))
                    .setSrcIndex(readableConfig.get(SRC_INDEX))
                    .setDstIndex(readableConfig.get(DST_INDEX))
                    .setRankIndex(readableConfig.get(RANK_INDEX))
                    .setFields(fieldList)
                    .setPositions(positionList)
                    .setWriteMode(writeMode)
                    .builder();
        }
        return executionOptions;
    }

    private NebulaClientOptions getNebulaClientOptions(ReadableConfig readableConfig) {
        return new NebulaClientOptions.NebulaClientOptionsBuilder()
                .setMetaAddress(readableConfig.get(METAADDRESS))
                .setGraphAddress(readableConfig.get(GRAPHADDRESS))
                .setUsername(readableConfig.get(USERNAME))
                .setPassword(readableConfig.get(PASSWORD))
                .build();
    }

    private void validateConfigOptions(ReadableConfig readableConfig) {
        String writeMode = readableConfig.get(WRITE_MODE);
        if (!WriteModeEnum.checkValidWriteMode(writeMode)) {
            throw new IllegalArgumentException(
                    String.format("Unknown sink.write-mode `%s`", writeMode)
            );
        }
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> set = new HashSet<>();
        set.add(METAADDRESS);
        set.add(GRAPHADDRESS);
        set.add(USERNAME);
        set.add(PASSWORD);
        return set;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> set = new HashSet<>();
        set.add(GRAPH_SPACE);
        set.add(LABEL_NAME);
        set.add(CONNECT_TIMEOUT);
        set.add(CONNECT_RETRY);
        set.add(TIMEOUT);
        set.add(EXECUTE_RETRY);
        set.add(SRC_INDEX);
        set.add(DST_INDEX);
        set.add(RANK_INDEX);
        return set;
    }
}
