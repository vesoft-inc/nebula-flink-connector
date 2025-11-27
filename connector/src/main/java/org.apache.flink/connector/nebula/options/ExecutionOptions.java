/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.options;

import java.io.Serializable;
import java.util.List;
import org.apache.flink.connector.nebula.utils.WriteModeEnum;

public class ExecutionOptions implements Serializable {
    private final String        graphName;
    private final List<String>  nebulaFields;
    private final List<String>  flinkFields;
    private final WriteModeEnum writeMode;
    private final int           batchSize;
    private final int           retryTimes;
    private final long          intervalMs;

    protected ExecutionOptions(String graphName,
                               List<String> nebulaFields,
                               List<String> flinkFields,
                               WriteModeEnum writeMode,
                               int batchSize,
                               int retryTimes,
                               long intervalMs) {
        this.graphName = graphName;
        this.nebulaFields = nebulaFields;
        this.flinkFields = flinkFields;
        this.writeMode = writeMode;
        this.batchSize = batchSize;
        this.retryTimes = retryTimes;
        this.intervalMs = intervalMs;
    }

    public String getGraphName() {
        return graphName;
    }

    public List<String> getNebulaFields() {
        return nebulaFields;
    }

    public List<String> getFlinkFields() {
        return flinkFields;
    }

    public WriteModeEnum getWriteMode() {
        return writeMode;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getRetryTimes() {
        return retryTimes;
    }

    public long getIntervalMs() {
        return intervalMs;
    }
}
