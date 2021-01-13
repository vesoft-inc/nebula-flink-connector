/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package org.apache.flink.connector.nebula.source;

import com.vesoft.nebula.client.storage.data.BaseTableRow;
import java.util.List;
import org.apache.flink.types.Row;

public class NebulaRowConverter implements NebulaConverter<Row> {

    @Override
    public Row convert(BaseTableRow row) {
        List<Object> values = row.getValues();
        Row record = new Row(values.size());
        for (int pos = 0; pos < values.size(); pos++) {
            record.setField(pos, values.get(pos));
        }
        return record;
    }
}
