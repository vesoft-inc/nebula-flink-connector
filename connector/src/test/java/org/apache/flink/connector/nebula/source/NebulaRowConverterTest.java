/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.source;

import com.vesoft.nebula.driver.graph.data.NPoint;
import com.vesoft.nebula.driver.graph.data.ValueWrapper;
import com.vesoft.nebula.driver.graph.decode.ColumnType;
import com.vesoft.nebula.driver.graph.scan.TableRow;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.connector.nebula.NebulaITTestBase;
import org.apache.flink.types.Row;
import org.junit.Test;

public class NebulaRowConverterTest extends NebulaITTestBase {

    @Test
    public void convertRow() {
        NebulaRowConverter converter = new NebulaRowConverter();
        List<ValueWrapper> values    = new ArrayList<>();
        values.add(new ValueWrapper(null, ColumnType.COLUMN_TYPE_NULL));
        values.add(new ValueWrapper(1, ColumnType.COLUMN_TYPE_UINT8));
        values.add(new ValueWrapper(2, ColumnType.COLUMN_TYPE_UINT16));
        values.add(new ValueWrapper(3, ColumnType.COLUMN_TYPE_UINT32));
        values.add(new ValueWrapper(10L, ColumnType.COLUMN_TYPE_UINT64));
        values.add(new ValueWrapper(1.0F, ColumnType.COLUMN_TYPE_FLOAT32));
        values.add(new ValueWrapper(2.0, ColumnType.COLUMN_TYPE_FLOAT64));
        values.add(new ValueWrapper(true, ColumnType.COLUMN_TYPE_BOOL));
        values.add(new ValueWrapper(LocalDate.of(2025, 1, 1),
                                    ColumnType.COLUMN_TYPE_DATE));
        values.add(new ValueWrapper(LocalDateTime.of(2024, 1, 1, 12, 20, 15, 30 * 1000),
                                    ColumnType.COLUMN_TYPE_LOCALDATETIME));
        values.add(new ValueWrapper(LocalTime.of(12, 20, 15, 30 * 1000),
                                    ColumnType.COLUMN_TYPE_LOCALTIME));
        values.add(new ValueWrapper(new BigDecimal("1.23456789"),
                                    ColumnType.COLUMN_TYPE_DECIMAL));
        values.add(new ValueWrapper(getList(), ColumnType.COLUMN_TYPE_LIST));
        values.add(new ValueWrapper(new NPoint(116.41667, 39.91667),
                                    ColumnType.COLUMN_TYPE_GEOGRAPHY));
        TableRow tableRow = new TableRow(values);
        Row      row      = converter.convert(tableRow);
        assert (row.getArity() == 14);
        assert (row.getField(0) == null);
        assert ((int) row.getField(1) == 1);
        assert ((int) row.getField(2) == 2);
        assert ((int) row.getField(3) == 3);
        assert ((long) row.getField(4) == 10L);
        assert ((float) row.getField(5) < 1.1);
        assert ((double) row.getField(6) < 2.1);
        assert ((boolean) row.getField(7));
        assert (((LocalDate) row.getField(8)).toString().equals("2025-01-01"));
        assert (((LocalDateTime) row.getField(9)).toString()
                .equals("2024-01-01T12:20:15.000030"));

        assert (((LocalTime) row.getField(10)).toString().equals("12:20:15.000030"));
        assert (((BigDecimal) row.getField(11)).toString().equals("1.23456789"));
        assert (((List) row.getField(12)).size() == 5);
        assert (((String) row.getField(13)).equals("POINT(116.41667 39.91667)"));
    }

    private List<ValueWrapper> getList() {
        List<ValueWrapper> values = new ArrayList<>();
        values.add(new ValueWrapper(1, ColumnType.COLUMN_TYPE_INT32));
        values.add(new ValueWrapper(2, ColumnType.COLUMN_TYPE_INT32));
        values.add(new ValueWrapper(3, ColumnType.COLUMN_TYPE_INT32));
        values.add(new ValueWrapper(4, ColumnType.COLUMN_TYPE_INT32));
        values.add(new ValueWrapper(5, ColumnType.COLUMN_TYPE_INT32));
        return values;
    }
}
