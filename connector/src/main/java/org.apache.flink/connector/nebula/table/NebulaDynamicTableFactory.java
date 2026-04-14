/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.table;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.nebula.options.ConnectionOptions;
import org.apache.flink.connector.nebula.options.ExecutionOptions;
import org.apache.flink.connector.nebula.options.SinkEdgeOptions;
import org.apache.flink.connector.nebula.options.SinkNodeOptions;
import org.apache.flink.connector.nebula.options.SourceEdgeOptions;
import org.apache.flink.connector.nebula.options.SourceExecutionOptions;
import org.apache.flink.connector.nebula.options.SourceNodeOptions;
import org.apache.flink.connector.nebula.sink.NebulaGqlTemplateEngine;
import org.apache.flink.connector.nebula.utils.DataTypeEnum;
import org.apache.flink.connector.nebula.utils.NebulaConstant;
import org.apache.flink.connector.nebula.utils.WriteModeEnum;
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

    public static final ConfigOption<String> GRAPH_NAME = ConfigOptions
            .key("graph-name")
            .stringType()
            .noDefaultValue()
            .withDescription("the nebula graph name.");

    public static final ConfigOption<String> LABEL_NAME = ConfigOptions
            .key("label-name")
            .stringType()
            .noDefaultValue()
            .withDescription("the nebula graph space label name.");

    public static final ConfigOption<WriteModeEnum> WRITE_MODE = ConfigOptions
            .key("write-mode")
            .enumType(WriteModeEnum.class)
            .defaultValue(WriteModeEnum.INSERTREPLACE)
            .withDescription("the write mode when save table into NebulaGraph.");
    public static final ConfigOption<DataTypeEnum>  DATA_TYPE  = ConfigOptions
            .key("data-type")
            .enumType(DataTypeEnum.class)
            .noDefaultValue()
            .withDescription("the nebula graph data type.");

    public static final ConfigOption<String> EDGE_PATTERN_TYPE = ConfigOptions
            .key("edge-pattern-type")
            .stringType()
            .noDefaultValue()
            .withDescription("edge pattern");

    public static final ConfigOption<String> SRC_NODE_TYPE = ConfigOptions
            .key("src-node-type")
            .stringType()
            .noDefaultValue()
            .withDescription("the source node type of edge");

    public static final ConfigOption<String> DST_NODE_TYPE = ConfigOptions
            .key("dst-node-type")
            .stringType()
            .noDefaultValue()
            .withDescription("the target node type of edge");

    public static final ConfigOption<Integer> TIMEOUT = ConfigOptions
            .key("timeout")
            .intType()
            .defaultValue(NebulaConstant.DEFAULT_REQUEST_TIMEOUT_MS)
            .withDescription("the nebula execute timeout duration.");

    public static final ConfigOption<String> PK_COLUMNS = ConfigOptions
            .key("pk-columns")
            .stringType()
            .noDefaultValue()
            .withDescription("the nebula node primary keys.");

    public static final ConfigOption<String> NODE_PKS = ConfigOptions
            .key("node-pks")
            .stringType()
            .defaultValue("")
            .withDescription("the nebula node primary keys.");

    public static final ConfigOption<String> SRC_PK_COLUMNS = ConfigOptions
            .key("src-pk-columns")
            .stringType()
            .noDefaultValue()
            .withDescription("the columns as source node primary keys, sep by comma.");

    public static final ConfigOption<String> EDGE_SRC_PKS = ConfigOptions
            .key("edge-src-pks")
            .stringType()
            .defaultValue("")
            .withDescription("the pks of source node type, sep by comma.");


    public static final ConfigOption<String> DST_PK_COLUMNS = ConfigOptions
            .key("dst-pk-columns")
            .stringType()
            .noDefaultValue()
            .withDescription("the columns as target node primary keys, sep by comma.");

    public static final ConfigOption<String> EDGE_DST_PKS = ConfigOptions
            .key("edge-dst-pks")
            .stringType()
            .defaultValue("")
            .withDescription("the pks of target node type, sep by comma.");


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

    public static final ConfigOption<String> GQL_TEMPLATE = ConfigOptions
            .key("gql-template")
            .stringType()
            .noDefaultValue()
            .withDescription("custom ngql template, must include {{TABLE}} placeholder.");

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig config = helper.getOptions();

        final DataType producedDataType =
                context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();

        helper.validate();
        validateConfigOptions(config);
        return new NebulaDynamicTableSink(getConnectionOptions(config),
                                          getExecutionOptions(context, config),
                                          producedDataType);
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
        SourceExecutionOptions executionOptions = getSourceExecutionOptions(context,
                                                                            readableConfig);
        ConnectionOptions connectionOptions = getConnectionOptions(readableConfig);
        return new NebulaDynamicTableSource(connectionOptions, executionOptions, physicalSchema);
    }

    private void validateConfigOptions(ReadableConfig config) {
        if (config.getOptional(TIMEOUT).isPresent() && config.get(TIMEOUT) < 0) {
            throw new IllegalArgumentException(
                    String.format("The value of '%s' option should not be negative, but is %s.",
                                  TIMEOUT.key(), config.get(TIMEOUT)));
        }
        if (config.getOptional(GQL_TEMPLATE).isPresent()
                && !config.get(GQL_TEMPLATE).contains(NebulaGqlTemplateEngine.TABLE_PLACEHOLDER)) {
            throw new IllegalArgumentException("gql-template must contain {{TABLE}} placeholder");
        }
    }

    private ConnectionOptions getConnectionOptions(ReadableConfig config) {
        return new ConnectionOptions.Builder()
                .withGraphAddress(config.get(GRAPHADDRESS))
                .withUser(config.get(USERNAME))
                .withPassword(config.get(PASSWORD))
                .build();
    }

    private ExecutionOptions getExecutionOptions(Context context, ReadableConfig config) {
        List<String> fields  = new ArrayList<>();
        List<Column> columns = context.getCatalogTable().getResolvedSchema().getColumns();

        String        labelName = config.get(LABEL_NAME);
        WriteModeEnum writeMode = config.get(WRITE_MODE);

        if (config.get(DATA_TYPE).isNode()) {
            for (int i = 0; i < columns.size(); i++) {
                fields.add(columns.get(i).getName());
            }

            SinkNodeOptions.Builder builder =
                    new SinkNodeOptions.Builder()
                            .withGraphName(config.get(GRAPH_NAME))
                            .withNodeType(labelName)
                            .withFlinkFields(fields)
                            .withNebulaFields(fields)
                            .withWriteMode(writeMode);
            config.getOptional(GQL_TEMPLATE).ifPresent(builder::withGqlTemplate);
            config.getOptional(BATCH_SIZE).ifPresent(builder::withBatchSize);
            config.getOptional(BATCH_INTERVAL_MS).ifPresent(builder::withIntervalMs);
            return builder.build();
        } else {
            List<String> flinkSrcNames = Arrays.asList(config.get(SRC_PK_COLUMNS).split(","));
            List<String> nebulaSrcPks  = Arrays.asList(config.get(EDGE_SRC_PKS).split(","));
            List<String> flinkDstNames = Arrays.asList(config.get(DST_PK_COLUMNS).split(","));
            List<String> nebulaDstPks  = Arrays.asList(config.get(EDGE_DST_PKS).split(","));
            for (int i = 0; i < columns.size(); i++) {
                if (flinkSrcNames.contains(columns.get(i).getName())
                        || flinkDstNames.contains(columns.get(i).getName())) {
                    continue;
                }
                fields.add(columns.get(i).getName());
            }

            SinkEdgeOptions.Builder builder =
                    new SinkEdgeOptions.Builder()
                            .withGraphName(config.get(GRAPH_NAME))
                            .withEdgeType(labelName)
                            .withFlinkFields(fields)
                            .withNebulaFields(fields)
                            .withFlinkSrcPkFields(flinkSrcNames)
                            .withFlinkDstPkFields(flinkDstNames)
                            .withNebulaSrcPks(nebulaSrcPks)
                            .withNebulaDstPks(nebulaDstPks)
                            .withWriteMode(writeMode);
            config.getOptional(GQL_TEMPLATE).ifPresent(builder::withGqlTemplate);
            config.getOptional(BATCH_SIZE).ifPresent(builder::withBatchSize);
            config.getOptional(BATCH_INTERVAL_MS).ifPresent(builder::withIntervalMs);
            return builder.build();
        }
    }


    private SourceExecutionOptions getSourceExecutionOptions(Context context,
                                                             ReadableConfig config) {
        List<String> fields  = new ArrayList<>();
        List<Column> columns = context.getCatalogTable().getResolvedSchema().getColumns();

        String labelName = config.get(LABEL_NAME);


        if (config.get(DATA_TYPE).isNode()) {
            for (Column column : columns) {
                fields.add(column.getName());
            }

            SourceNodeOptions.Builder builder =
                    new SourceNodeOptions.Builder()
                            .withGraphName(config.get(GRAPH_NAME))
                            .withNodeType(labelName)
                            .withReturnCols(fields);
            config.getOptional(BATCH_SIZE).ifPresent(builder::withBatchSize);
            return builder.build();
        } else {
            List<String> flinkSrcNames = Arrays.asList(config.getOptional(SRC_PK_COLUMNS)
                                                               .orElse("").split(","));
            List<String> flinkDstNames = Arrays.asList(config.getOptional(DST_PK_COLUMNS)
                                                               .orElse("").split(","));
            for (Column column : columns) {
                if (flinkSrcNames.contains(column.getName())
                        || flinkDstNames.contains(column.getName())) {
                    continue;
                }
                fields.add(column.getName());
            }

            SourceEdgeOptions.Builder builder =
                    new SourceEdgeOptions.Builder()
                            .withGraphName(config.get(GRAPH_NAME))
                            .withEdgeType(labelName)
                            .withReturnCols(fields);
            config.getOptional(BATCH_SIZE).ifPresent(builder::withBatchSize);
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
        set.add(GRAPH_NAME);
        set.add(LABEL_NAME);
        set.add(DATA_TYPE);
        set.add(GRAPHADDRESS);
        set.add(USERNAME);
        set.add(PASSWORD);
        return set;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> set = new HashSet<>();
        set.add(TIMEOUT);
        set.add(NODE_PKS);
        set.add(EDGE_SRC_PKS);
        set.add(EDGE_DST_PKS);
        set.add(BATCH_SIZE);
        set.add(BATCH_INTERVAL_MS);
        set.add(SRC_PK_COLUMNS);
        set.add(DST_PK_COLUMNS);
        set.add(WRITE_MODE);
        set.add(PK_COLUMNS);
        set.add(GQL_TEMPLATE);
        return set;
    }
}
