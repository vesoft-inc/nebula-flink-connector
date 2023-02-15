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
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
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

    public static final ConfigOption<DataTypeEnum> DATA_TYPE = ConfigOptions
            .key("data-type")
            .enumType(DataTypeEnum.class)
            .noDefaultValue()
            .withDescription("the nebula graph data type.");

    public static final ConfigOption<Integer> TIMEOUT = ConfigOptions
            .key("timeout")
            .intType()
            .defaultValue(NebulaConstant.DEFAULT_TIMEOUT_MS)
            .withDescription("the nebula execute timeout duration.");

    public static final ConfigOption<Integer> ID_INDEX = ConfigOptions
            .key("id-index")
            .intType()
            .defaultValue(NebulaConstant.DEFAULT_VERTEX_ID_INDEX)
            .withDescription("the nebula execute vertex index.");

    public static final ConfigOption<Integer> SRC_ID_INDEX = ConfigOptions
            .key("src-id-index")
            .intType()
            .defaultValue(NebulaConstant.DEFAULT_ROW_INFO_INDEX)
            .withDescription("the nebula execute edge src index.");

    public static final ConfigOption<Integer> DST_ID_INDEX = ConfigOptions
            .key("dst-id-index")
            .intType()
            .defaultValue(NebulaConstant.DEFAULT_ROW_INFO_INDEX)
            .withDescription("the nebula execute edge dst index.");

    public static final ConfigOption<Integer> RANK_ID_INDEX = ConfigOptions
            .key("rank-id-index")
            .intType()
            .defaultValue(NebulaConstant.DEFAULT_ROW_INFO_INDEX)
            .withDescription("the nebula execute rank index.");

    public static final ConfigOption<Integer> BATCH_SIZE = ConfigOptions
            .key("batch-size")
            .intType()
            .noDefaultValue()
            .withDescription("batch size.");

    public static final ConfigOption<Integer> BATCH_INTERVAL_MS = ConfigOptions
            .key("batch-interval-ms")
            .intType()
            .noDefaultValue()
            .withDescription("batch commit interval in milliseconds.");

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig config = helper.getOptions();

        final DataType producedDataType =
                context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();

        helper.validate();
        validateConfigOptions(config);
        return new NebulaDynamicTableSink(
                getClientOptions(config), getExecutionOptions(context, config), producedDataType);
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig readableConfig = helper.getOptions();
        helper.validate();
        validateConfigOptions(readableConfig);
        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        ExecutionOptions executionOptions = getExecutionOptions(context, readableConfig);
        NebulaClientOptions nebulaClientOptions = getClientOptions(readableConfig);
        return new NebulaDynamicTableSource(nebulaClientOptions, executionOptions, physicalSchema);
    }

    private void validateConfigOptions(ReadableConfig config) {
        if (config.getOptional(TIMEOUT).isPresent() && config.get(TIMEOUT) < 0) {
            throw new IllegalArgumentException(
                    String.format("The value of '%s' option should not be negative, but is %s.",
                            TIMEOUT.key(), config.get(TIMEOUT)));
        }
    }

    private NebulaClientOptions getClientOptions(ReadableConfig config) {
        return new NebulaClientOptions.NebulaClientOptionsBuilder()
                .setMetaAddress(config.get(METAADDRESS))
                .setGraphAddress(config.get(GRAPHADDRESS))
                .setUsername(config.get(USERNAME))
                .setPassword(config.get(PASSWORD))
                .build();
    }

    private ExecutionOptions getExecutionOptions(Context context, ReadableConfig config) {
        List<String> fields = new ArrayList<>();
        List<Integer> positions = new ArrayList<>();
        List<Column> columns = context.getCatalogTable().getResolvedSchema().getColumns();
        String labelName = config.get(LABEL_NAME);

        if (config.get(DATA_TYPE).isVertex()) {
            for (int i = 1; i < columns.size(); i++) {
                positions.add(i);
                fields.add(columns.get(i).getName());
            }

            VertexExecutionOptions.ExecutionOptionBuilder builder =
                    new VertexExecutionOptions.ExecutionOptionBuilder()
                            .setFields(fields)
                            .setIdIndex(config.get(ID_INDEX))
                            .setPositions(positions)
                            .setGraphSpace(config.get(GRAPH_SPACE))
                            .setTag(labelName);
            config.getOptional(BATCH_SIZE).ifPresent(builder::setBatchSize);
            config.getOptional(BATCH_INTERVAL_MS).ifPresent(builder::setBatchIntervalMs);
            return builder.build();
        } else {
            for (int i = 2; i < columns.size(); i++) {
                if (config.get(RANK_ID_INDEX) != i) {
                    positions.add(i);
                    fields.add(columns.get(i).getName());
                }
            }

            EdgeExecutionOptions.ExecutionOptionBuilder builder =
                    new EdgeExecutionOptions.ExecutionOptionBuilder()
                            .setFields(fields)
                            .setSrcIndex(config.get(SRC_ID_INDEX))
                            .setDstIndex(config.get(DST_ID_INDEX))
                            .setRankIndex(config.get(RANK_ID_INDEX))
                            .setPositions(positions)
                            .setGraphSpace(config.get(GRAPH_SPACE))
                            .setEdge(labelName);
            config.getOptional(BATCH_SIZE).ifPresent(builder::setBatchSize);
            config.getOptional(BATCH_INTERVAL_MS).ifPresent(builder::setBatchIntervalMs);
            return builder.build();
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
        set.add(DATA_TYPE);
        set.add(TIMEOUT);
        set.add(ID_INDEX);
        set.add(SRC_ID_INDEX);
        set.add(DST_ID_INDEX);
        set.add(RANK_ID_INDEX);
        set.add(BATCH_SIZE);
        set.add(BATCH_INTERVAL_MS);
        return set;
    }
}
