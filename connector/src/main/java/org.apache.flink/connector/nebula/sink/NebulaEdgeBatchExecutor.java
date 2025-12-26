/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.sink;


import com.vesoft.nebula.driver.graph.data.ResultSet;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.connector.nebula.connection.GraphProvider;
import org.apache.flink.connector.nebula.options.SinkEdgeOptions;
import org.apache.flink.connector.nebula.utils.NebulaEdge;
import org.apache.flink.connector.nebula.utils.NebulaEdgeSchema;
import org.apache.flink.connector.nebula.utils.NebulaEdges;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NebulaEdgeBatchExecutor implements NebulaBatchExecutor<Row> {
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
        if (nebulaEdgeList.size() == 0) {
            return null;
        }
        NebulaEdges nebulaEdges = new NebulaEdges(schema, nebulaEdgeList);
        // generate the write ngql statement
        String statement = null;
        switch (executionOptions.getWriteMode()) {
            case INSERT:
            case INSERTIGNORE:
            case INSERTREPLACE:
            case INSERTUPDATE:
                statement = nebulaEdges.getInsertStatement(executionOptions.getGraphName(),
                                                           executionOptions.getWriteMode(),
                                                           executionOptions.getFlinkSrcPkFields(),
                                                           executionOptions.getNebulaSrcPks(),
                                                           executionOptions.getFlinkDstPkFields(),
                                                           executionOptions.getNebulaDstPks(),
                                                           executionOptions.getFlinkFields(),
                                                           executionOptions.getNebulaFields());
                break;
            case UPDATE:
                statement = nebulaEdges.getUpdateStatement(executionOptions.getGraphName(),
                                                           executionOptions.getFlinkSrcPkFields(),
                                                           executionOptions.getNebulaSrcPks(),
                                                           executionOptions.getFlinkDstPkFields(),
                                                           executionOptions.getNebulaDstPks(),
                                                           executionOptions.getFlinkFields(),
                                                           executionOptions.getNebulaFields());
                break;
            case DELETE:
                statement = nebulaEdges.getDeleteStatement(executionOptions.getGraphName(),
                                                           executionOptions.getFlinkSrcPkFields(),
                                                           executionOptions.getNebulaSrcPks(),
                                                           executionOptions.getFlinkDstPkFields(),
                                                           executionOptions.getNebulaDstPks());
                break;
            default:
                throw new IllegalArgumentException("write mode is not supported");
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
            LOG.error(">>>>>> write data error, ", e);
            if (executionOptions.throwErrorWhenFailed()) {
                throw new RuntimeException("write edge failed", e);
            }
            nebulaEdgeList.clear();
            return statement;
        }

        if (execResult.isSucceeded()) {
            LOG.info(">>>>> write edge {} succeed, latency:{{}}ms, response:{{}}ms",
                     executionOptions.getEdgeType(),
                     execResult.getLatency() / 1000.0,
                     (end - start));
        } else {
            LOG.error(">>>>> write edge failed: {}", execResult.getErrorMessage());
            LOG.error(">>>>> failed gql: {}", statement);
            if (executionOptions.throwErrorWhenFailed()) {
                throw new RuntimeException("write edge failed:" + execResult.getErrorMessage());
            }
            nebulaEdgeList.clear();
            return statement;
        }
        nebulaEdgeList.clear();
        return null;
    }
}
