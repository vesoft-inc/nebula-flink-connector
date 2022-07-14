/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.source;

import com.vesoft.nebula.client.graph.data.ValueWrapper;
import com.vesoft.nebula.client.storage.data.BaseTableRow;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.List;
import org.apache.flink.types.Row;

/**
 * convert nebula {@link BaseTableRow} to flink {@link Row}
 */
public class NebulaRowConverter implements NebulaConverter<Row>, Serializable {

    private static final long serialVersionUID = 7823753627300856104L;

    @Override
    public Row convert(BaseTableRow row) throws UnsupportedEncodingException {
        List<ValueWrapper> values = row.getValues();
        Row record = new Row(values.size());
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
            if (valueWrapper.isLong()) {
                record.setField(pos, valueWrapper.asLong());
                continue;
            }
            if (valueWrapper.isDouble()) {
                record.setField(pos, valueWrapper.asDouble());
                continue;
            }
            if (valueWrapper.isDate()) {
                record.setField(pos, valueWrapper.asDate());
                continue;
            }
            if (valueWrapper.isTime()) {
                record.setField(pos, valueWrapper.asTime());
                continue;
            }
            if (valueWrapper.isDateTime()) {
                record.setField(pos, valueWrapper.asDateTime());
                continue;
            }
            if (valueWrapper.isGeography()) {
                record.setField(pos, valueWrapper.asGeography());
                continue;
            }
            if (valueWrapper.isDuration()) {
                record.setField(pos, valueWrapper.asDuration());
            }
        }
        return record;
    }
}
