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

public class NebulaRowEdgeOutputFormatConverter implements NebulaOutputFormatConverter<Row> {

    private final int srcIdIndex;
    private final int dstIdIndex;
    private final int rankIndex;
    private final VidTypeEnum vidType;
    private final List<Integer> positions;
    private final Map<Integer, String> pos2Field;
    private final Map<String, Integer> schema;

    public NebulaRowEdgeOutputFormatConverter(EdgeExecutionOptions executionOptions,
                                              VidTypeEnum vidType,
                                              Map<String, Integer> schema) {
        this.srcIdIndex = executionOptions.getSrcIndex();
        this.dstIdIndex = executionOptions.getDstIndex();
        this.rankIndex = executionOptions.getRankIndex();
        this.vidType = vidType;
        this.schema = schema;
        this.positions = executionOptions.getPositions();
        this.pos2Field = new HashMap<>();
        List<String> fields = executionOptions.getFields();
        for (int i = 0; i < positions.size(); i++) {
            this.pos2Field.put(positions.get(i), fields.get(i));
        }
    }

    @Override
    public String createValue(Row row, PolicyEnum policy) {
        if (row == null || row.getArity() == 0) {
            Log.error("empty row");
            return null;
        }

        Object srcId = row.getField(srcIdIndex);
        Object dstId = row.getField(dstIdIndex);
        if (srcId == null || dstId == null) {
            return null;
        }
        List<String> edgeProps = new ArrayList<>();
        for (int i : positions) {
            String propName = pos2Field.get(i);
            int type = schema.get(propName);
            edgeProps.add(NebulaUtils.extraValue(row.getField(i), type));
        }

        String srcFormatId = srcId.toString();
        String dstFormatId = dstId.toString();

        if (policy == null) {
            if (vidType == VidTypeEnum.STRING) {
                srcFormatId = NebulaUtils.mkString(srcFormatId, "\"", "", "\"");
                dstFormatId = NebulaUtils.mkString(dstFormatId, "\"", "", "\"");
            } else {
                assert (NebulaUtils.isNumeric(srcFormatId));
                assert (NebulaUtils.isNumeric(dstFormatId));
            }
        } else {
            assert (vidType == VidTypeEnum.INT);
            srcFormatId = String.format(NebulaConstant.ENDPOINT_TEMPLATE, policy.policy(),
                    srcId.toString());
            dstFormatId = String.format(NebulaConstant.ENDPOINT_TEMPLATE, policy.policy(),
                    dstId.toString());
        }

        if (rankIndex >= 0) {
            assert row.getField(rankIndex) != null;
            Long rank = Long.parseLong(row.getField(rankIndex).toString());
            return String.format(NebulaConstant.EDGE_VALUE_TEMPLATE, srcFormatId, dstFormatId,
                    rank, String.join(NebulaConstant.COMMA, edgeProps));
        } else {
            return String.format(NebulaConstant.EDGE_VALUE_WITHOUT_RANKING_TEMPLATE, srcFormatId,
                    dstFormatId, String.join(NebulaConstant.COMMA, edgeProps));
        }
    }
}
