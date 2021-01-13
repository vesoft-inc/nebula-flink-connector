/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package org.apache.flink.connector.nebula.statement;

import static org.apache.flink.connector.nebula.utils.NebulaConstant.DEFAULT_ROW_INFO_INDEX;
import static org.apache.flink.connector.nebula.utils.NebulaConstant.DEFAULT_SCAN_LIMIT;
import static org.apache.flink.connector.nebula.utils.NebulaConstant.DEFAULT_WRITE_BATCH;

import java.util.List;
import org.apache.flink.connector.nebula.utils.DataTypeEnum;
import org.apache.flink.connector.nebula.utils.PolicyEnum;

public class EdgeExecutionOptions extends ExecutionOptions {

    /**
     * nebula edge type
     */
    private String edge;

    /**
     * src index for nebula edge sink
     */
    private int srcIndex;

    /**
     * dst index for nebula edge sink
     */
    private int dstIndex;

    /**
     * rank index for nebula edge sink
     */
    private int rankIndex;


    private EdgeExecutionOptions(String graphSpace, String executeStatement, List<String> fields,
                                 List<Integer> positions, boolean noColumn, int limit,
                                 long startTime, long endTime, long batch, PolicyEnum policy,
                                 String edge, int srcIndex, int dstIndex, int rankIndex) {
        super(graphSpace, executeStatement, fields, positions, noColumn, limit, startTime,
                endTime, batch, policy);
        this.edge = edge;
        this.srcIndex = srcIndex;
        this.dstIndex = dstIndex;
        this.rankIndex = rankIndex;
    }

    public String getEdge() {
        return edge;
    }

    public int getSrcIndex() {
        return srcIndex;
    }

    public int getDstIndex() {
        return dstIndex;
    }

    public int getRankIndex() {
        return rankIndex;
    }

    @Override
    public String getLabel() {
        return edge;
    }

    @Override
    public DataTypeEnum getDataType() {
        return DataTypeEnum.EDGE;
    }

    public static class ExecutionOptionBuilder {
        private String graphSpace;
        private String executeStatement;
        private String edge;
        private List<String> fields;
        private List<Integer> positions;
        private boolean noColumn = false;
        private int limit = DEFAULT_SCAN_LIMIT;
        private long startTime = 0;
        private long endTime = Long.MAX_VALUE;
        private int batch = DEFAULT_WRITE_BATCH;
        private PolicyEnum policy = null;
        private int srcIndex = DEFAULT_ROW_INFO_INDEX;
        private int dstIndex = DEFAULT_ROW_INFO_INDEX;
        private int rankIndex = DEFAULT_ROW_INFO_INDEX;

        public ExecutionOptionBuilder setGraphSpace(String graphSpace) {
            this.graphSpace = graphSpace;
            return this;
        }

        public ExecutionOptionBuilder setExecuteStatement(String executeStatement) {
            this.executeStatement = executeStatement;
            return this;
        }

        public ExecutionOptionBuilder setEdge(String edge) {
            this.edge = edge;
            return this;
        }

        public ExecutionOptionBuilder setFields(List<String> fields) {
            this.fields = fields;
            return this;
        }

        public ExecutionOptionBuilder setPositions(List<Integer> positions) {
            this.positions = positions;
            return this;
        }

        public ExecutionOptionBuilder setNoColumn(boolean noColumn) {
            this.noColumn = noColumn;
            return this;
        }

        public ExecutionOptionBuilder setLimit(int limit) {
            this.limit = limit;
            return this;
        }

        public ExecutionOptionBuilder setStartTime(long startTime) {
            this.startTime = startTime;
            return this;
        }

        public ExecutionOptionBuilder setEndTime(long endTime) {
            this.endTime = endTime;
            return this;
        }

        public ExecutionOptionBuilder setBatch(int batch) {
            this.batch = batch;
            return this;
        }

        public ExecutionOptionBuilder setPolicy(String policy) {
            if (policy != null || !policy.trim().isEmpty()) {
                this.policy = PolicyEnum.valueOf(policy);
            }
            return this;
        }

        public ExecutionOptionBuilder setSrcIndex(int srcIndex) {
            this.srcIndex = srcIndex;
            return this;
        }

        public ExecutionOptionBuilder setDstIndex(int dstIndex) {
            this.dstIndex = dstIndex;
            return this;
        }

        public ExecutionOptionBuilder setRankIndex(int rankIndex) {
            this.rankIndex = rankIndex;
            return this;
        }

        public ExecutionOptions builder() {
            if (graphSpace == null || graphSpace.trim().isEmpty()) {
                throw new IllegalArgumentException("graph space can not be empty.");
            }
            if (edge == null || edge.trim().isEmpty()) {
                throw new IllegalArgumentException("edge can not be empty.");
            }
            return new EdgeExecutionOptions(graphSpace, executeStatement, fields, positions,
                    noColumn, limit, startTime, endTime, batch, policy, edge, srcIndex, dstIndex,
                    rankIndex);
        }
    }
}
