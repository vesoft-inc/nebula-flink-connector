/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.statement;

import java.io.Serializable;
import java.util.List;
import org.apache.flink.connector.nebula.utils.DataTypeEnum;
import org.apache.flink.connector.nebula.utils.FailureHandlerEnum;
import org.apache.flink.connector.nebula.utils.PolicyEnum;
import org.apache.flink.connector.nebula.utils.WriteModeEnum;
import org.apache.flink.types.Row;

/**
 * NebulaGraph sink and source options
 *
 * <p>for NebulaGraph Vertex Sink
 *
 * <pre><code>
 *
 * ExecutionOptions executionOptions = new VertexExecutionOptions.ExecutionOptionBuilder()
 *                 .setGraphSpace("flinkSink")
 *                 .setTag("player")
 *                 .setIdIndex(0)
 *                 .setFields(Arrays.asList("name", "age"))
 *                 .setPositions(Arrays.asList(1, 2))
 *                 .setBatchSize(100)
 *                 .setPolicy("hash")
 *                 .build();
 *
 * </code></pre>
 *
 * <p>for NebulaGraph Edge Sink
 *
 * <pre><code>
 * ExecutionOptions executionOptions = new EdgeExecutionOptions.ExecutionOptionBuilder()
 *                 .setGraphSpace("flinkSink")
 *                 .setEdge("friend")
 *                 .setSrcIndex(0)
 *                 .setDstIndex(1)
 *                 .setRankIndex(2)
 *                 .setFields(Arrays.asList("src", "dst", "degree", "start"))
 *                 .setPositions(Arrays.asList(0, 1, 3, 4))
 *                 .setBatchSize(2)
 *                 .build();
 *
 * </code></pre>
 *
 * <p>for NebulaGraph Vertex Source
 * <pre><code>
 * ExecutionOptions executionOptions = new VertexExecutionOptions.ExecutionOptionBuilder()
 *                 .setGraphSpace("flinkSink")
 *                 .setTag("player")
 *                 .setFields(Arrays.asList("name", "age"))
 *                 .setLimit(100)
 *                 .build();
 * </code></pre>
 *
 * <p>for NebulaGraph Edge Source
 * <pre><code>
 * ExecutionOptions executionOptions1 = new EdgeExecutionOptions.ExecutionOptionBuilder()
 *                 .setGraphSpace("flinkSink")
 *                 .setEdge("friend")
 *                 .setFields(Arrays.asList("name", "age"))
 *                 //.setLimit(100)
 *                 //.setStartTime(0)
 *                 //.setEndTime(Long.MAX_VALUE)
 *                 .build();
 * </code></pre>
 *
 * @see Row
 */
public abstract class ExecutionOptions implements Serializable {
    private static final long serialVersionUID = 6958907525999542402L;

    /**
     * nebula graph space
     */
    private String graphSpace;


    /**
     * execute statement without return data
     */
    private String executeStatement;

    /**
     * fields of one label
     */
    private List<String> fields;

    /**
     * positions of one label
     * position and field are corresponding
     */
    private List<Integer> positions;

    /**
     * read no property
     */
    private boolean noColumn;

    /**
     * data amount one scan for read
     */
    private int limit;

    /**
     * parameter for scan operator
     */
    private long startTime;

    /**
     * parameter for scan operator
     */
    private long endTime;

    /**
     * data amount one batch for insert
     */
    private int batchSize;

    /**
     * policy for vertexId or edge src、 dst, see {@link PolicyEnum}
     */
    private PolicyEnum policy;

    /**
     * write mode
     */
    private WriteModeEnum writeMode;

    /**
     * interval between write submit
     */
    private int batchIntervalMs;

    /**
     * failure handler
     */
    private FailureHandlerEnum failureHandler;

    /**
     * maximum number of retries
     */
    private int maxRetries;

    /**
     * retry delay
     */
    private int retryDelayMs;

    protected ExecutionOptions(String graphSpace,
                               String executeStatement,
                               List<String> fields,
                               List<Integer> positions,
                               boolean noColumn,
                               int limit,
                               long startTime,
                               long endTime,
                               int batchSize,
                               PolicyEnum policy,
                               WriteModeEnum writeMode,
                               int batchIntervalMs,
                               FailureHandlerEnum failureHandler,
                               int maxRetries,
                               int retryDelayMs) {
        this.graphSpace = graphSpace;
        this.executeStatement = executeStatement;
        this.fields = fields;
        this.positions = positions;
        this.noColumn = noColumn;
        this.limit = limit;
        this.startTime = startTime;
        this.endTime = endTime;
        this.batchSize = batchSize;
        this.policy = policy;
        this.writeMode = writeMode;
        this.batchIntervalMs = batchIntervalMs;
        this.failureHandler = failureHandler;
        this.maxRetries = maxRetries;
        this.retryDelayMs = retryDelayMs;
    }

    public String getGraphSpace() {
        return graphSpace;
    }

    public String getExecuteStatement() {
        return executeStatement;
    }

    public List<String> getFields() {
        return fields;
    }

    public List<Integer> getPositions() {
        return positions;
    }

    public boolean isNoColumn() {
        return noColumn;
    }

    public int getLimit() {
        return limit;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    @Deprecated
    public int getBatch() {
        return batchSize;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public PolicyEnum getPolicy() {
        return policy;
    }

    public abstract String getLabel();

    public abstract DataTypeEnum getDataType();

    public WriteModeEnum getWriteMode() {
        return writeMode;
    }

    public int getBatchIntervalMs() {
        return batchIntervalMs;
    }

    public FailureHandlerEnum getFailureHandler() {
        return failureHandler;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public int getRetryDelayMs() {
        return retryDelayMs;
    }

    @Override
    public String toString() {
        return "ExecutionOptions{"
                + "graphSpace='" + graphSpace + '\''
                + ", executeStatement='" + executeStatement + '\''
                + ", fields=" + fields
                + ", positions=" + positions
                + ", noColumn=" + noColumn
                + ", limit=" + limit
                + ", startTime=" + startTime
                + ", endTime=" + endTime
                + ", batchSize=" + batchSize
                + ", policy=" + policy
                + ", mode=" + writeMode
                + ", batchIntervalMs=" + batchIntervalMs
                + ", failureHandler=" + failureHandler
                + ", maxRetries=" + maxRetries
                + ", retryDelayMs=" + retryDelayMs
                + '}';
    }
}
