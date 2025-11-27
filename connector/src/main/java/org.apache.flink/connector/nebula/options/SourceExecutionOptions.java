/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.options;

import java.io.Serializable;
import java.util.List;

public class SourceExecutionOptions implements Serializable {
    private final String       schema;
    private final String       graphName;
    private final String       typeName;
    private final List<String> returnCols;
    private final int          batchSize;

    protected SourceExecutionOptions(String schema,
                                     String graphName,
                                     String typeName,
                                     List<String> returnCols,
                                     int batchSize) {
        this.schema = schema;
        this.graphName = graphName;
        this.typeName = typeName;
        this.returnCols = returnCols;
        this.batchSize = batchSize;
    }

    public String getSchema() {
        return schema;
    }

    public String getGraphName() {
        return graphName;
    }

    public String getTypeName() {
        return typeName;
    }

    public List<String> getReturnCols() {
        return returnCols;
    }

    public int getBatchSize() {
        return batchSize;
    }


}
