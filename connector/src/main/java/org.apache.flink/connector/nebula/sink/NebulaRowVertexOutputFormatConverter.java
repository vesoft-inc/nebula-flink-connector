/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.sink;

import com.esotericsoftware.minlog.Log;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.connector.nebula.statement.VertexExecutionOptions;
import org.apache.flink.connector.nebula.utils.NebulaUtils;
import org.apache.flink.connector.nebula.utils.NebulaVertex;
import org.apache.flink.connector.nebula.utils.PolicyEnum;
import org.apache.flink.connector.nebula.utils.VidTypeEnum;
import org.apache.flink.types.Row;

public class NebulaRowVertexOutputFormatConverter implements Serializable {

    private static final long serialVersionUID = -7728344698410737677L;

    private final int idIndex;
    private final VidTypeEnum vidType;
    private final List<Integer> positions;
    private final Map<Integer, String> pos2Field;
    private final Map<String, Integer> schema;


    public NebulaRowVertexOutputFormatConverter(VertexExecutionOptions executionOptions,
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


    public NebulaVertex createVertex(Row row, PolicyEnum policy) {
        // check row data
        if (row == null || row.getArity() == 0) {
            Log.error("empty row");
            return null;
        }
        Object id = row.getField(idIndex);
        if (id == null) {
            Log.error("wrong id, your id is null ");
            return null;
        }
        // extract vertex properties
        List<String> vertexProps = new ArrayList<>();
        for (int i : positions) {
            String propName = pos2Field.get(i);
            if (propName == null || !schema.containsKey(propName)) {
                throw new IllegalArgumentException("position " + i + " or field " + propName
                        + " does not exist.");
            }
            int type = schema.get(propName);
            vertexProps.add(NebulaUtils.extraValue(row.getField(i), type));
        }

        // format vertex id
        String formatId = String.valueOf(id);
        if (policy == null) {
            if (vidType == VidTypeEnum.STRING) {
                formatId = NebulaUtils.mkString(NebulaUtils.escapeUtil(String.valueOf(formatId)),
                        "\"", "", "\"");
            } else {
                assert (NebulaUtils.isNumeric(formatId));
            }
        } else {
            assert (vidType == VidTypeEnum.INT);
        }
        NebulaVertex vertex = new NebulaVertex(formatId, vertexProps);
        return vertex;
    }
}
