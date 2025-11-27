/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.sink;

import com.esotericsoftware.minlog.Log;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.connector.nebula.options.SinkNodeOptions;
import org.apache.flink.connector.nebula.utils.NebulaNode;
import org.apache.flink.connector.nebula.utils.NebulaNodeSchema;
import org.apache.flink.connector.nebula.utils.NebulaUtils;
import org.apache.flink.types.Row;

public class NebulaRowNodeOutputFormatConverter implements Serializable {

    private static final long serialVersionUID = -7728344698410737677L;

    private final NebulaNodeSchema schema;
    private final SinkNodeOptions  sinkNodeOptions;

    private final List<String> flinkFields;
    private final List<String> nebulaFields;


    public NebulaRowNodeOutputFormatConverter(SinkNodeOptions sinkNodeOptions,
                                              NebulaNodeSchema schema) {
        this.sinkNodeOptions = sinkNodeOptions;
        this.flinkFields = sinkNodeOptions.getFlinkFields();
        this.nebulaFields = sinkNodeOptions.getNebulaFields();
        this.schema = schema;
    }


    public NebulaNode createNode(Row row) {
        // check row data
        if (row == null || row.getArity() == 0) {
            Log.error("empty row");
            return null;
        }
        for (String pk : schema.getPkNames()) {
            Object pkValue = row.getField(flinkFields.get(nebulaFields.indexOf(pk)));
            if (pkValue == null) {
                Log.warn(String.format("primary key %s is null. row:%s", pk, row.toString()));
                return null;
            }
        }

        // extract vertex properties
        Map<String, String> vertexProps = new HashMap<>();
        for (int i = 0; i < nebulaFields.size(); i++) {
            String propName   = nebulaFields.get(i);
            Object flinkValue = row.getField(flinkFields.get(i));
            String value      = flinkValue == null ? null : flinkValue.toString();
            String dataType   = schema.getProperties().get(propName);
            vertexProps.put(propName, NebulaUtils.extractValue(dataType, value, null));
        }

        return new NebulaNode(vertexProps);
    }
}
