/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.source;

import com.vesoft.nebula.driver.graph.scan.TableRow;

public class NebulaBaseTableRowConverter implements NebulaConverter<TableRow> {

    @Override
    public TableRow convert(TableRow row) {
        return row;
    }

}
