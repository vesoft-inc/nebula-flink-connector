/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package org.apache.flink.connector.nebula.sink;

import com.esotericsoftware.minlog.Log;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.connector.nebula.statement.EdgeExecutionOptions;
import org.apache.flink.connector.nebula.utils.NebulaConstant;
import org.apache.flink.connector.nebula.utils.NebulaUtils;
import org.apache.flink.connector.nebula.utils.PolicyEnum;
import org.apache.flink.connector.nebula.utils.VidTypeEnum;
import org.apache.flink.types.Row;

public class NebulaRowEdgeOutputFormatConverter extends NebulaEdgeOutputFormatConverter<Row> {

    public NebulaRowEdgeOutputFormatConverter(EdgeExecutionOptions executionOptions,
                                              VidTypeEnum vidType,
                                              Map<String, Integer> schema) {
        super(executionOptions, vidType, schema);
    }

    @Override
    public boolean checkInvalid(Row row) {
        return row == null || row.getArity() == 0;
    }

    @Override
    public Object extractSrcId(Row row, Integer srcIdIndex) {
        return row.getField(srcIdIndex);
    }

    @Override
    public Object extractDstId(Row row, Integer dstIdIndex) {
        return row.getField(dstIdIndex);
    }

    @Override
    public Object extractRank(Row row, Integer rankIndex) {
        return row.getField(rankIndex);
    }

    @Override
    public Object extractProperty(Row row, Integer propertyIndex) {
        return row.getField(propertyIndex);
    }
}
