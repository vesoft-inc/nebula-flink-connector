/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.sink;

import com.vesoft.nebula.driver.graph.data.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.connector.nebula.connection.GraphProvider;
import org.apache.flink.connector.nebula.options.SinkNodeOptions;
import org.apache.flink.connector.nebula.utils.NebulaNode;
import org.apache.flink.connector.nebula.utils.NebulaNodeSchema;
import org.apache.flink.connector.nebula.utils.NebulaNodes;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NebulaNodeBatchExecutor implements NebulaBatchExecutor<Row> {
    private static final Logger LOG = LoggerFactory.getLogger(NebulaNodeBatchExecutor.class);

    private final SinkNodeOptions                    executionOptions;
    private final List<NebulaNode>                   nebulaVertexList;
    private final NebulaRowNodeOutputFormatConverter converter;
    private final NebulaNodeSchema                   schema;

    public NebulaNodeBatchExecutor(SinkNodeOptions executionOptions,
                                   NebulaNodeSchema schema) {
        this.executionOptions = executionOptions;
        this.nebulaVertexList = new ArrayList<>();
        this.schema = schema;
        this.converter = new NebulaRowNodeOutputFormatConverter(executionOptions, schema);
    }

    /**
     * put record into buffer
     *
     * @param record represent vertex or edge
     */
    @Override
    public void addToBatch(Row record) {
        NebulaNode vertex = converter.createNode(record);
        if (vertex == null) {
            LOG.error(">>>>> vertex is null. maybe the row is invalid.");
            return;
        }
        nebulaVertexList.add(vertex);
    }

    @Override
    public String executeBatch(GraphProvider graphProvider) {
        if (nebulaVertexList.size() == 0) {
            return null;
        }
        NebulaNodes nebulaNodes = new NebulaNodes(schema, nebulaVertexList);
        // generate the write ngql statement
        String statement;
        if (executionOptions.hasCustomGqlTemplate()) {
            Map<String, String> values = new HashMap<>();
            values.put("TABLE",
                       nebulaNodes.buildTableClause(executionOptions.getFlinkFields(),
                                                    executionOptions.getNebulaFields()));
            values.put("GRAPH", executionOptions.getGraphName());
            values.put("TYPE", executionOptions.getNodeType());
            values.put("LABEL", executionOptions.getNodeType());
            values.put("WRITE_MODE", executionOptions.getWriteMode().name());
            values.put("WRITE_MODE_NGQL", executionOptions.getWriteMode().getMode());
            statement = NebulaGqlTemplateEngine.render(executionOptions.getGqlTemplate(), values);
        } else {
            statement = null;
            switch (executionOptions.getWriteMode()) {
                case INSERT:
                case INSERTIGNORE:
                case INSERTREPLACE:
                case INSERTUPDATE:
                    statement = nebulaNodes.getInsertStatement(executionOptions.getGraphName(),
                                                               executionOptions.getWriteMode(),
                                                               executionOptions.getFlinkFields(),
                                                               executionOptions.getNebulaFields());
                    break;
                case UPDATE:
                    statement = nebulaNodes.getUpdateStatement(executionOptions.getGraphName(),
                                                               executionOptions.getFlinkFields(),
                                                               executionOptions.getNebulaFields());
                    break;
                case DELETE:
                case DETACHDELETE:
                    statement = nebulaNodes.getDeleteStatement(executionOptions.getGraphName(),
                                                               executionOptions.getWriteMode(),
                                                               executionOptions.getFlinkFields(),
                                                               executionOptions.getNebulaFields());
                    break;
                default:
                    throw new IllegalArgumentException("write mode is not supported");
            }
        }

        // execute ngql statement
        ResultSet execResult = null;
        long      start;
        long      end;
        try {
            start = System.currentTimeMillis();
            execResult = graphProvider.execute(statement);
            end = System.currentTimeMillis();
        } catch (Exception e) {
            LOG.error("write data error, ", e);
            if (executionOptions.throwErrorWhenFailed()) {
                throw new RuntimeException("write node failed", e);
            }
            nebulaVertexList.clear();
            return statement;
        }

        if (execResult.isSucceeded()) {
            LOG.info(">>>>> write node {} succeed, latency:{{}}ms, response:{{}}ms",
                     executionOptions.getNodeType(),
                     execResult.getLatency() / 1000.0,
                     (end - start));
        } else {
            LOG.error(">>>>> write data failed: {}", execResult.getErrorMessage());
            LOG.error(">>>>> failed gql: {}", statement);
            if (executionOptions.throwErrorWhenFailed()) {
                throw new RuntimeException("write node failed:" + execResult.getErrorMessage());
            }
            nebulaVertexList.clear();
            return statement;
        }
        nebulaVertexList.clear();
        return null;
    }
}
