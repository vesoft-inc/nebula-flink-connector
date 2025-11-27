/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.options;

import static org.apache.flink.connector.nebula.utils.NebulaConstant.DEFAULT_INTERVAL_MILLIS;
import static org.apache.flink.connector.nebula.utils.NebulaConstant.DEFAULT_RETRY_TIMES;
import static org.apache.flink.connector.nebula.utils.NebulaConstant.DEFAULT_WRITE_BATCH_SIZE;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.connector.nebula.utils.WriteModeEnum;

public class SinkNodeOptions extends ExecutionOptions {

    private       Builder builder;
    private final String  nodeType;

    private SinkNodeOptions(Builder builder) {
        super(builder.graphName, builder.nebulaFields, builder.flinkFields,
              builder.writeMode, builder.batchSize, builder.retryTimes, builder.intervalMs);
        this.builder = builder;
        this.nodeType = builder.nodeType;
    }

    public String getNodeType() {
        return nodeType;
    }

    public static Builder builder() {
        return new Builder();
    }

    public Builder toBuilder() {
        return builder;
    }

    public static final class Builder implements Serializable {
        private long intervalMs = DEFAULT_INTERVAL_MILLIS;

        private String        graphName;
        private String        nodeType;
        private List<String>  nebulaFields = new ArrayList<>();
        private List<String>  flinkFields  = new ArrayList<>();
        private WriteModeEnum writeMode    = WriteModeEnum.INSERTREPLACE;
        private int           batchSize    = DEFAULT_WRITE_BATCH_SIZE;

        private int retryTimes = DEFAULT_RETRY_TIMES;

        public Builder withGraphName(String graphName) {
            this.graphName = graphName;
            return this;
        }

        public Builder withNodeType(String nodeType) {
            this.nodeType = nodeType;
            return this;
        }

        public Builder withNebulaFields(List<String> nebulaFields) {
            this.nebulaFields = nebulaFields;
            return this;
        }

        public Builder withFlinkFields(List<String> flinkFields) {
            this.flinkFields = flinkFields;
            return this;
        }

        public Builder withWriteMode(WriteModeEnum writeMode) {
            this.writeMode = writeMode;
            return this;
        }

        public Builder withBatchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder withRetryTimes(int retryTimes) {
            this.retryTimes = retryTimes;
            return this;
        }

        public Builder withIntervalMs(long intervalMs) {
            this.intervalMs = intervalMs;
            return this;
        }

        private void check() {
            if (nebulaFields.size() != flinkFields.size()) {
                throw new IllegalArgumentException(
                        "nebulaFields and flinkFields has different elements number");
            }
        }

        public SinkNodeOptions build() {
            check();
            return new SinkNodeOptions(this);
        }
    }
}
