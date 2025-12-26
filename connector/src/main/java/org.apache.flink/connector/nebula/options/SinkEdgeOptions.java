/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.options;

import static org.apache.flink.connector.nebula.utils.NebulaConstant.DEFAULT_ERROR_WHEN_FAILED;
import static org.apache.flink.connector.nebula.utils.NebulaConstant.DEFAULT_INTERVAL_MILLIS;
import static org.apache.flink.connector.nebula.utils.NebulaConstant.DEFAULT_RETRY_TIMES;
import static org.apache.flink.connector.nebula.utils.NebulaConstant.DEFAULT_WRITE_BATCH_SIZE;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.connector.nebula.utils.WriteModeEnum;

public class SinkEdgeOptions extends ExecutionOptions {
    private final Builder builder;

    private final String       edgeType;
    private final List<String> nebulaSrcPks;
    private final List<String> flinkSrcPkFields;
    private final List<String> nebulaDstPks;
    private final List<String> flinkDstPkFields;


    private SinkEdgeOptions(Builder builder) {
        super(builder.graphName,
              builder.nebulaFields,
              builder.flinkFields,
              builder.writeMode,
              builder.batchSize,
              builder.retryTimes,
              builder.intervalMs,
              builder.errorWhenFailed);
        this.builder = builder;
        this.edgeType = builder.edgeType;
        this.nebulaSrcPks = builder.nebulaSrcPks;
        this.flinkSrcPkFields = builder.flinkSrcPkFields;
        this.nebulaDstPks = builder.nebulaDstPks;
        this.flinkDstPkFields = builder.flinkDstPkFields;
    }


    public String getEdgeType() {
        return edgeType;
    }


    public List<String> getNebulaSrcPks() {
        return nebulaSrcPks;
    }

    public List<String> getFlinkSrcPkFields() {
        return flinkSrcPkFields;
    }

    public List<String> getNebulaDstPks() {
        return nebulaDstPks;
    }

    public List<String> getFlinkDstPkFields() {
        return flinkDstPkFields;
    }

    public Builder toBuilder() {
        return builder;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder implements Serializable {
        private String        graphName;
        private String        edgeType;
        private List<String>  nebulaFields     = new ArrayList<>();
        private List<String>  flinkFields      = new ArrayList<>();
        private List<String>  nebulaSrcPks     = new ArrayList<>();
        private List<String>  flinkSrcPkFields = new ArrayList<>();
        private List<String>  nebulaDstPks     = new ArrayList<>();
        private List<String>  flinkDstPkFields = new ArrayList<>();
        private WriteModeEnum writeMode        = WriteModeEnum.INSERTREPLACE;
        private int           batchSize        = DEFAULT_WRITE_BATCH_SIZE;
        private int           retryTimes       = DEFAULT_RETRY_TIMES;
        private long          intervalMs       = DEFAULT_INTERVAL_MILLIS;
        private boolean       errorWhenFailed  = DEFAULT_ERROR_WHEN_FAILED;


        public Builder withGraphName(String graphName) {
            this.graphName = graphName;
            return this;
        }

        public Builder withEdgeType(String edgeType) {
            this.edgeType = edgeType;
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

        public Builder withNebulaSrcPks(List<String> nebulaSrcPks) {
            this.nebulaSrcPks = nebulaSrcPks;
            return this;
        }

        public Builder withFlinkSrcPkFields(List<String> flinkSrcPkFields) {
            this.flinkSrcPkFields = flinkSrcPkFields;
            return this;
        }

        public Builder withNebulaDstPks(List<String> nebulaDstPks) {
            this.nebulaDstPks = nebulaDstPks;
            return this;
        }

        public Builder withFlinkDstPkFields(List<String> flinkDstPkFields) {
            this.flinkDstPkFields = flinkDstPkFields;
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
            if (retryTimes < 0) {
                this.retryTimes = 0;
            } else {
                this.retryTimes = retryTimes;
            }
            return this;
        }

        public Builder withIntervalMs(long intervalMs) {
            if (intervalMs < 0) {
                this.intervalMs = 0;
            } else {
                this.intervalMs = intervalMs;
            }
            return this;
        }

        public Builder withErrorWhenFailed(boolean errorWhenFailed) {
            this.errorWhenFailed = errorWhenFailed;
            return this;
        }

        private void check() {
            if (nebulaFields.size() != flinkFields.size()) {
                throw new IllegalArgumentException(
                        "nebulaFields and flinkFields has different elements number");
            }
            if (batchSize <= 0) {
                batchSize = DEFAULT_WRITE_BATCH_SIZE;
            }

            if (flinkSrcPkFields.isEmpty()) {
                throw new IllegalArgumentException("flinkSrcPkFields not configured");
            }
            if (flinkDstPkFields.isEmpty()) {
                throw new IllegalArgumentException("flinkDstPkFields not configured");
            }

            if (flinkSrcPkFields.size() > 1 && (nebulaSrcPks.size() != flinkSrcPkFields.size())) {
                throw new IllegalArgumentException(
                        "nebulaSrcPks and flinkSrcPkFields has different elements number");
            }
            if (flinkDstPkFields.size() > 1 && (nebulaDstPks.size() != flinkDstPkFields.size())) {
                throw new IllegalArgumentException(
                        "nebulaDstPks and flinkDstPkFields has different elements number");
            }
        }

        public SinkEdgeOptions build() {
            check();
            return new SinkEdgeOptions(this);
        }
    }
}
