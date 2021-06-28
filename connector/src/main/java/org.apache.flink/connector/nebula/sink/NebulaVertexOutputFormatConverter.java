package org.apache.flink.connector.nebula.sink;

import com.esotericsoftware.minlog.Log;
import org.apache.flink.connector.nebula.statement.VertexExecutionOptions;
import org.apache.flink.connector.nebula.utils.NebulaConstant;
import org.apache.flink.connector.nebula.utils.NebulaUtils;
import org.apache.flink.connector.nebula.utils.PolicyEnum;
import org.apache.flink.connector.nebula.utils.VidTypeEnum;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class NebulaVertexOutputFormatConverter<T> implements NebulaOutputFormatConverter<T>{

    private final int idIndex;
    private final VidTypeEnum vidType;
    private final List<Integer> positions;
    private final Map<Integer, String> pos2Field;
    private final Map<String, Integer> schema;


    public NebulaVertexOutputFormatConverter(VertexExecutionOptions executionOptions,
                                                VidTypeEnum vidType,
                                                Map<String, Integer> schema) {
        this.idIndex = executionOptions.getIdIndex();
        this.vidType = vidType;
        this.positions = executionOptions.getPositions();
        this.pos2Field = new HashMap<>();
        List<String> fields = executionOptions.getFields();
        for (int i = 0; i < positions.size(); i++) {
            this.pos2Field.put(positions.get(i), fields.get(i));
        }
        this.schema = schema;
    }

    @Override
    public String createValue(T record, PolicyEnum policy) {
        if (checkInvalid(record)) {
            Log.error("wrong record");
            return null;
        }
        Object id = extractId(record, idIndex);
        if (id == null) {
            return null;
        }
        List<String> vertexProps = new ArrayList<>();
        for (int i : positions) {
            String propName = pos2Field.get(i);
            if (propName == null || !schema.containsKey(propName)) {
                throw new IllegalArgumentException("position " + i + " or field " + propName
                        + "does not exist.");
            }
            int type = schema.get(propName);
            vertexProps.add(NebulaUtils.extraValue(extractProperty(record, i), type));
        }

        String formatId = String.valueOf(id);

        if (policy == null) {
            if (vidType == VidTypeEnum.STRING) {
                formatId = NebulaUtils.mkString(formatId, "\"", "", "\"");
            } else {
                assert (NebulaUtils.isNumeric(formatId));
            }
            return String.format(NebulaConstant.VERTEX_VALUE_TEMPLATE, formatId,
                    String.join(",", vertexProps));
        } else {
            assert (vidType == VidTypeEnum.INT);
            return String.format(NebulaConstant.VERTEX_VALUE_TEMPLATE_WITH_POLICY,
                    policy.policy(), formatId, String.join(",", vertexProps));
        }

    }

    public abstract boolean checkInvalid(T record);

    public abstract Object extractId(T record, Integer idIndex);

    public abstract Object extractProperty(T record, Integer propertyIndex);
}
