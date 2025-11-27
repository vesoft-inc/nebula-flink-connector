/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.options;

import static org.apache.flink.connector.nebula.utils.NebulaConstant.DEFAULT_SCAN_BATCH_SIZE;

import java.util.ArrayList;
import java.util.List;

public class SourceNodeOptions extends SourceExecutionOptions {


    private SourceNodeOptions(String schema,
                              String graphName,
                              String nodeType,
                              List<String> returnCols,
                              int batchSize) {
        super(schema, graphName, nodeType, returnCols, batchSize);
    }


    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String       schema;
        private String       graphName;
        private String       nodeType;
        private List<String> returnCols = new ArrayList<>();
        private int          batchSize  = DEFAULT_SCAN_BATCH_SIZE;

        public Builder withSchema(String schema) {
            this.schema = schema;
            return this;
        }

        public Builder withGraphName(String graphName) {
            this.graphName = graphName;
            return this;
        }

        public Builder withNodeType(String nodeType) {
            this.nodeType = nodeType;
            return this;
        }

        public Builder withReturnCols(List<String> returnCols) {
            this.returnCols = returnCols;
            return this;
        }

        public Builder withBatchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public SourceNodeOptions build() {
            return new SourceNodeOptions(
                    schema,
                    graphName,
                    nodeType,
                    returnCols,
                    batchSize);
        }
    }
}
