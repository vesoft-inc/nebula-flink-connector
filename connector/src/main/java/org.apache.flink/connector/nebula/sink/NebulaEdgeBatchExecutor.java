/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.sink;


import com.vesoft.nebula.driver.graph.ErrorCode;
import com.vesoft.nebula.driver.graph.data.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.connector.nebula.connection.GraphProvider;
import org.apache.flink.connector.nebula.options.SinkEdgeOptions;
import org.apache.flink.connector.nebula.utils.NebulaEdge;
import org.apache.flink.connector.nebula.utils.NebulaEdgeSchema;
import org.apache.flink.connector.nebula.utils.NebulaEdges;
import org.apache.flink.connector.nebula.utils.WriteModeEnum;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NebulaEdgeBatchExecutor extends AbstractNebulaRetryableBatchExecutor<NebulaEdge>
        implements NebulaBatchExecutor<Row> {
    private static final Logger LOG = LoggerFactory.getLogger(NebulaEdgeBatchExecutor.class);

    private final SinkEdgeOptions                    executionOptions;
    private final List<NebulaEdge>                   nebulaEdgeList;
    private final NebulaEdgeSchema                   schema;
    private final NebulaRowEdgeOutputFormatConverter converter;

    public NebulaEdgeBatchExecutor(SinkEdgeOptions executionOptions,
                                   NebulaEdgeSchema schema) {
        this.executionOptions = executionOptions;
        this.schema = schema;
        this.nebulaEdgeList = new ArrayList<>();
        this.converter = new NebulaRowEdgeOutputFormatConverter(executionOptions, schema);
    }

    /**
     * put record into buffer
     */
    @Override
    public void addToBatch(Row record) {
        NebulaEdge edge = converter.createEdge(record);

        if (edge == null) {
            LOG.warn(">>>>> edge is null. maybe the row is invalid.");
            return;
        }
        nebulaEdgeList.add(edge);
    }


    @Override
    public String executeBatch(GraphProvider graphProvider) {
        if (nebulaEdgeList.isEmpty()) {
            return null;
        }
        try {
            return writeEntities(new ArrayList<>(nebulaEdgeList), graphProvider);
        } finally {
            nebulaEdgeList.clear();
        }
    }

    @Override
    protected String getGql(List<NebulaEdge> edges) {
        NebulaEdges nebulaEdges = new NebulaEdges(schema, edges);
        if (executionOptions.hasCustomGqlTemplate()) {
            Map<String, String> values = new HashMap<>();
            values.put("TABLE",
                       nebulaEdges.buildTableClause(executionOptions.getFlinkSrcPkFields(),
                                                    executionOptions.getNebulaSrcPks(),
                                                    executionOptions.getFlinkDstPkFields(),
                                                    executionOptions.getNebulaDstPks(),
                                                    executionOptions.getFlinkFields(),
                                                    executionOptions.getNebulaFields()));
            values.put("GRAPH", executionOptions.getGraphName());
            values.put("TYPE", executionOptions.getEdgeType());
            values.put("LABEL", executionOptions.getEdgeType());
            values.put("WRITE_MODE", executionOptions.getWriteMode().name());
            values.put("WRITE_MODE_NGQL", executionOptions.getWriteMode().getMode());
            return NebulaGqlTemplateEngine.render(executionOptions.getGqlTemplate(), values);
        }

        List<String> flinkSrcPkFields = executionOptions.getFlinkSrcPkFields();
        List<String> nebulaSrcPks = executionOptions.getNebulaSrcPks();
        List<String> flinkDstPkFields = executionOptions.getFlinkDstPkFields();
        List<String> nebulaDstPks = executionOptions.getNebulaDstPks();
        List<String> flinkFields = executionOptions.getFlinkFields();
        List<String> nebulaFields = executionOptions.getNebulaFields();
        switch (executionOptions.getWriteMode()) {
            case INSERT:
            case INSERTIGNORE:
            case INSERTREPLACE:
            case INSERTUPDATE:
                return nebulaEdges.getInsertStatement(executionOptions.getGraphName(),
                                                      executionOptions.getWriteMode(),
                                                      flinkSrcPkFields,
                                                      nebulaSrcPks,
                                                      flinkDstPkFields,
                                                      nebulaDstPks,
                                                      flinkFields,
                                                      nebulaFields);
            case UPDATE:
                return nebulaEdges.getUpdateStatement(executionOptions.getGraphName(),
                                                      flinkSrcPkFields,
                                                      nebulaSrcPks,
                                                      flinkDstPkFields,
                                                      nebulaDstPks,
                                                      flinkFields,
                                                      nebulaFields);
            case DELETE:
                return nebulaEdges.getDeleteStatement(executionOptions.getGraphName(),
                                                      flinkSrcPkFields,
                                                      nebulaSrcPks,
                                                      flinkDstPkFields,
                                                      nebulaDstPks);
            default:
                throw new IllegalArgumentException("write mode is not supported");
        }
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }

    @Override
    protected String getEntityName() {
        return "edge";
    }

    @Override
    protected String getWriteTarget() {
        return executionOptions.getEdgeType();
    }

    @Override
    protected WriteModeEnum getWriteMode() {
        return executionOptions.getWriteMode();
    }

    @Override
    protected boolean throwErrorWhenFailed() {
        return executionOptions.throwErrorWhenFailed();
    }

    @Override
    protected int getRetryTimes() {
        return executionOptions.getRetryTimes();
    }

    @Override
    protected long getRetryIntervalMs() {
        return executionOptions.getIntervalMs();
    }

    @Override
    protected boolean isAlreadyExist(ResultSet resultSet) {
        return "EDGE_ALREADY_EXIST".equals(resultSet.getErrorCode().name());
    }

    @Override
    protected long getAffectedCount(ResultSet resultSet) {
        return resultSet.getExtraInfo() == null ? 0L : resultSet.getExtraInfo().getAffectedEdges();
    }
}
