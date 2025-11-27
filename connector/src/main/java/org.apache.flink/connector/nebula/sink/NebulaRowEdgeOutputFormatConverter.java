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
import org.apache.flink.connector.nebula.options.SinkEdgeOptions;
import org.apache.flink.connector.nebula.utils.NebulaEdge;
import org.apache.flink.connector.nebula.utils.NebulaEdgeSchema;
import org.apache.flink.connector.nebula.utils.NebulaUtils;
import org.apache.flink.types.Row;

public class NebulaRowEdgeOutputFormatConverter implements Serializable {
    private final List<String>     flinkFields;
    private final List<String>     nebulaFields;
    private final List<String>     flinkSrcPkFields;
    private final List<String>     nebulaSrcPks;
    private final List<String>     flinkDstPkFields;
    private final List<String>     nebulaDstPks;
    private final SinkEdgeOptions  executionOptions;
    private final NebulaEdgeSchema schema;

    public NebulaRowEdgeOutputFormatConverter(SinkEdgeOptions executionOptions,
                                              NebulaEdgeSchema schema) {
        this.flinkFields = executionOptions.getFlinkFields();
        this.nebulaFields = executionOptions.getNebulaFields();
        this.flinkSrcPkFields = executionOptions.getFlinkSrcPkFields();
        this.nebulaSrcPks = executionOptions.getNebulaSrcPks();
        this.flinkDstPkFields = executionOptions.getFlinkDstPkFields();
        this.nebulaDstPks = executionOptions.getNebulaDstPks();
        this.executionOptions = executionOptions;
        this.schema = schema;
    }


    public NebulaEdge createEdge(Row row) {
        // check row data
        if (row == null || row.getArity() == 0) {
            Log.error("empty row");
            return null;
        }

        for (String srcPk : schema.getSrcPkNames()) {
            Object pkValue = row.getField(flinkSrcPkFields.get(nebulaSrcPks.indexOf(srcPk)));
            if (pkValue == null) {
                Log.warn(String.format("primary key %s of source node %s for %s is null. row:%s",
                                       srcPk,
                                       schema.getSrcNodeTypeName(),
                                       schema.getEdgeTypeName(),
                                       row));
                return null;
            }
        }
        for (String dstPk : schema.getDstPkNames()) {
            Object pkValue = row.getField(flinkDstPkFields.get(nebulaDstPks.indexOf(dstPk)));
            if (pkValue == null) {
                Log.warn(String.format("primary key %s of target node %s for %s is null. row:%s",
                                       dstPk,
                                       schema.getDstNodeTypeName(),
                                       schema.getEdgeTypeName(),
                                       row));
                return null;
            }
        }

        Map<String, String> srcPks = new HashMap<>();
        for (int i = 0; i < nebulaSrcPks.size(); i++) {
            String pkName     = nebulaSrcPks.get(i);
            String dataType   = schema.getSrcPkDataTypeMap().get(pkName);
            Object flinkValue = row.getField(flinkSrcPkFields.get(i));
            String value      = flinkValue == null ? null : flinkValue.toString();
            srcPks.put(pkName, NebulaUtils.extractValue(dataType, value, null));
        }


        Map<String, String> dstPks = new HashMap<>();
        for (int i = 0; i < nebulaDstPks.size(); i++) {
            String pkName     = nebulaDstPks.get(i);
            String dataType   = schema.getDstPkDataTypeMap().get(pkName);
            Object flinkValue = row.getField(flinkDstPkFields.get(i));
            String value      = flinkValue == null ? null : flinkValue.toString();
            dstPks.put(pkName, NebulaUtils.extractValue(dataType, value, null));
        }

        // extract edge properties
        Map<String, String> edgeProps = new HashMap<>();
        for (int i = 0; i < nebulaFields.size(); i++) {
            String dataType   = schema.getProperties().get(nebulaFields.get(i));
            Object flinkValue = row.getField(flinkFields.get(i));
            String value      = flinkValue == null ? null : flinkValue.toString();
            edgeProps.put(nebulaFields.get(i), NebulaUtils.extractValue(dataType, value, null));
        }
        NebulaEdge edge = new NebulaEdge(srcPks, dstPks, edgeProps);
        return edge;
    }
}
