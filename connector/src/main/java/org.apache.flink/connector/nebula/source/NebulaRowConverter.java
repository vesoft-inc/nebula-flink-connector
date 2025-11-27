/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.source;

import com.vesoft.nebula.driver.graph.data.ValueWrapper;
import com.vesoft.nebula.driver.graph.scan.TableRow;
import java.util.List;
import org.apache.flink.types.Row;

/**
 * convert nebula {@link TableRow} to flink {@link Row}
 */
public class NebulaRowConverter implements NebulaConverter<Row> {

    @Override
    public Row convert(TableRow row) {
        List<ValueWrapper> values = row.getValues();
        Row                record = new Row(values.size());
        for (int pos = 0; pos < values.size(); pos++) {
            ValueWrapper valueWrapper = values.get(pos);

            if (valueWrapper.isNull()) {
                record.setField(pos, null);
                continue;
            }
            if (valueWrapper.isString()) {
                record.setField(pos, valueWrapper.asString());
                continue;
            }
            if (valueWrapper.isBoolean()) {
                record.setField(pos, valueWrapper.asBoolean());
                continue;
            }
            if (valueWrapper.isInt()) {
                record.setField(pos, valueWrapper.asInt());
                continue;
            }
            if (valueWrapper.isLong()) {
                record.setField(pos, valueWrapper.asLong());
                continue;
            }
            if (valueWrapper.isDouble()) {
                record.setField(pos, valueWrapper.asDouble());
                continue;
            }
            if (valueWrapper.isFloat()) {
                record.setField(pos, valueWrapper.asFloat());
                continue;
            }
            if (valueWrapper.isDate()) {
                record.setField(pos, valueWrapper.asDate());
                continue;
            }
            if (valueWrapper.isLocalTime()) {
                record.setField(pos, valueWrapper.asLocalTime());
                continue;
            }
            if (valueWrapper.isLocalDateTime()) {
                record.setField(pos, valueWrapper.asLocalDateTime());
                continue;
            }
            if (valueWrapper.isZonedTime()) {
                record.setField(pos, valueWrapper.asZonedTime());
                continue;
            }
            if (valueWrapper.isZonedDateTime()) {
                record.setField(pos, valueWrapper.asZonedDateTime());
                continue;
            }
            if (valueWrapper.isDecimal()) {
                record.setField(pos, valueWrapper.asDecimal());
                continue;
            }
            if (valueWrapper.isDuration()) {
                record.setField(pos, valueWrapper.asDuration().toString());
                continue;
            }
            if (valueWrapper.isList()) {
                record.setField(pos, valueWrapper.asList());
                continue;
            }
            if (valueWrapper.isVector()) {
                record.setField(pos, valueWrapper.asVector().toString());
                continue;
            }
            if (valueWrapper.isRecord()) {
                record.setField(pos, valueWrapper.asRecord().toString());
                continue;
            }
            if (valueWrapper.isNode()) {
                record.setField(pos, valueWrapper.asNode().toString());
                continue;
            }
            if (valueWrapper.isEdge()) {
                record.setField(pos, valueWrapper.asEdge().toString());
                continue;
            }
            if (valueWrapper.isPath()) {
                record.setField(pos, valueWrapper.asPath().toString());
                continue;
            }
            if (valueWrapper.isGeography()) {
                record.setField(pos, valueWrapper.asGeography().toString());
            }
        }
        return record;
    }
}
