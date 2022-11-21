/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.statement;

import static org.apache.flink.connector.nebula.utils.NebulaConstant.DEFAULT_BATCH_INTERVAL_MS;
import static org.apache.flink.connector.nebula.utils.NebulaConstant.DEFAULT_ROW_INFO_INDEX;
import static org.apache.flink.connector.nebula.utils.NebulaConstant.DEFAULT_SCAN_LIMIT;
import static org.apache.flink.connector.nebula.utils.NebulaConstant.DEFAULT_WRITE_BATCH;

import java.util.List;
import org.apache.flink.connector.nebula.utils.DataTypeEnum;
import org.apache.flink.connector.nebula.utils.PolicyEnum;
import org.apache.flink.connector.nebula.utils.WriteModeEnum;

public class VertexExecutionOptions extends ExecutionOptions {

    /**
     * nebula vertex tag
     */
    private String tag;

    /**
     * id index for nebula vertex sink
     */
    private int idIndex;

    public VertexExecutionOptions(String graphSpace,
                                  String executeStatement,
                                  List<String> fields,
                                  List<Integer> positions,
                                  boolean noColumn,
                                  int limit,
                                  long startTime,
                                  long endTime,
                                  long batch,
                                  PolicyEnum policy,
                                  WriteModeEnum mode,
                                  String tag,
                                  int idIndex,
                                  long batchIntervalMs) {
        super(graphSpace, executeStatement, fields, positions, noColumn, limit, startTime,
                endTime, batch, policy, mode, batchIntervalMs);
        this.tag = tag;
        this.idIndex = idIndex;
    }

    public int getIdIndex() {
        return idIndex;
    }

    @Override
    public String getLabel() {
        return tag;
    }

    @Override
    public DataTypeEnum getDataType() {
        return DataTypeEnum.VERTEX;
    }

    public static class ExecutionOptionBuilder {
        private String graphSpace;
        private String executeStatement;
        private String tag;
        private List<String> fields;
        private List<Integer> positions;
        private boolean noColumn = false;
        private int limit = DEFAULT_SCAN_LIMIT;
        private long startTime = 0;
        private long endTime = Long.MAX_VALUE;
        private int batch = DEFAULT_WRITE_BATCH;
        private long batchIntervalMs = DEFAULT_BATCH_INTERVAL_MS;
        private PolicyEnum policy = null;
        private WriteModeEnum mode = WriteModeEnum.INSERT;
        private int idIndex = DEFAULT_ROW_INFO_INDEX;

        public ExecutionOptionBuilder setGraphSpace(String graphSpace) {
            this.graphSpace = graphSpace;
            return this;
        }

        public ExecutionOptionBuilder setExecuteStatement(String executeStatement) {
            this.executeStatement = executeStatement;
            return this;
        }

        public ExecutionOptionBuilder setTag(String tag) {
            this.tag = tag;
            return this;
        }

        public ExecutionOptionBuilder setFields(List<String> fields) {
            this.fields = fields;
            return this;
        }

        /**
         * positions in flink row for each field of setFields(fields)
         */
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

        public ExecutionOptionBuilder setIdIndex(int idIndex) {
            this.idIndex = idIndex;
            return this;
        }

        public ExecutionOptionBuilder setWriteMode(WriteModeEnum mode) {
            this.mode = mode;
            return this;
        }

        public ExecutionOptionBuilder setBatchIntervalMs(long batchIntervalMs) {
            this.batchIntervalMs = batchIntervalMs;
            return this;
        }

        public ExecutionOptions builder() {
            if (graphSpace == null || graphSpace.trim().isEmpty()) {
                throw new IllegalArgumentException("graph space can not be empty.");
            }
            if (tag == null || tag.trim().isEmpty()) {
                throw new IllegalArgumentException("tag can not be empty.");
            }
            return new VertexExecutionOptions(graphSpace, executeStatement, fields,
                    positions, noColumn, limit, startTime, endTime, batch, policy, mode, tag,
                    idIndex, batchIntervalMs);
        }
    }
}
