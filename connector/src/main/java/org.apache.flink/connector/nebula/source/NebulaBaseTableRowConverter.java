/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.source;

import com.vesoft.nebula.client.storage.data.BaseTableRow;

public class NebulaBaseTableRowConverter implements NebulaConverter<BaseTableRow> {

    @Override
    public BaseTableRow convert(BaseTableRow row) {
        return row;
    }

}
