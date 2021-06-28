package org.apache.flink.connector.nebula.sink;

import com.esotericsoftware.minlog.Log;
import org.apache.flink.connector.nebula.statement.EdgeExecutionOptions;
import org.apache.flink.connector.nebula.utils.NebulaConstant;
import org.apache.flink.connector.nebula.utils.NebulaUtils;
import org.apache.flink.connector.nebula.utils.PolicyEnum;
import org.apache.flink.connector.nebula.utils.VidTypeEnum;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class NebulaEdgeOutputFormatConverter<T> implements NebulaOutputFormatConverter<T>{

    private final int srcIdIndex;
    private final int dstIdIndex;
    private final int rankIndex;
    private final VidTypeEnum vidType;
    private final List<Integer> positions;
    private final Map<Integer, String> pos2Field;
    private final Map<String, Integer> schema;

    public NebulaEdgeOutputFormatConverter(EdgeExecutionOptions executionOptions,
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
    public String createValue(T record, PolicyEnum policy) {
        if (checkInvalid(record)) {
            Log.error("wrong record");
            return null;
        }

        Object srcId = extractSrcId(record, srcIdIndex);
        Object dstId = extractDstId(record, dstIdIndex);
        if (srcId == null || dstId == null) {
            return null;
        }
        List<String> edgeProps = new ArrayList<>();
        for (int i : positions) {
            String propName = pos2Field.get(i);
            int type = schema.get(propName);
            edgeProps.add(NebulaUtils.extraValue(extractProperty(record, i), type));
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
            assert extractRank(record, rankIndex) != null;
            Long rank = Long.parseLong(extractRank(record, rankIndex).toString());
            return String.format(NebulaConstant.EDGE_VALUE_TEMPLATE, srcFormatId, dstFormatId,
                    rank, String.join(NebulaConstant.COMMA, edgeProps));
        } else {
            return String.format(NebulaConstant.EDGE_VALUE_WITHOUT_RANKING_TEMPLATE, srcFormatId,
                    dstFormatId, String.join(NebulaConstant.COMMA, edgeProps));
        }
    }

    public abstract boolean checkInvalid(T record);

    public abstract Object extractSrcId(T record, Integer srcIdIndex);

    public abstract Object extractDstId(T record, Integer dstIdIndex);

    public abstract Object extractRank(T record, Integer rankIndex);

    public abstract Object extractProperty(T record, Integer propertyIndex);
}
