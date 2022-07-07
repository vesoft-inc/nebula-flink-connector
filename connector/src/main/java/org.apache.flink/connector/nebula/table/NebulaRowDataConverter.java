/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.table;

import com.vesoft.nebula.client.graph.data.DateTimeWrapper;
import com.vesoft.nebula.client.graph.data.DateWrapper;
import com.vesoft.nebula.client.graph.data.TimeWrapper;
import com.vesoft.nebula.client.graph.data.ValueWrapper;
import com.vesoft.nebula.client.storage.data.BaseTableRow;
import org.apache.flink.connector.nebula.source.NebulaConverter;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * convert nebula {@link BaseTableRow} to flink {@link RowData}
 */
public class NebulaRowDataConverter implements NebulaConverter<RowData> {

    private final RowType rowType;
    private final NebulaDeserializationConverter[] toInternalConverters;
    private final NebulaSerializationConverter[] toExternalConverters;
    private final LogicalType[] fieldTypes;

    public NebulaRowDataConverter(RowType rowType) {
        this.rowType = checkNotNull(rowType);
        this.fieldTypes = rowType.getFields().stream()
                .map(RowType.RowField::getType)
                .toArray(LogicalType[]::new);
        this.toInternalConverters = new NebulaDeserializationConverter[rowType.getFieldCount()];
        this.toExternalConverters = new NebulaSerializationConverter[rowType.getFieldCount()];

        for (int i = 0; i < rowType.getFieldCount(); i++) {
            this.toInternalConverters[i] = createInternalConverter(fieldTypes[i]);
            this.toExternalConverters[i] = createExternalConverter(fieldTypes[i]);
        }
    }

    @Override
    public RowData convert(BaseTableRow record) throws UnsupportedEncodingException {
        List<ValueWrapper> values = record.getValues();
        GenericRowData genericRowData = new GenericRowData(rowType.getFieldCount());
        for (int pos = 0; pos < rowType.getFieldCount(); pos++) {
            ValueWrapper valueWrapper = values.get(pos);
            if (valueWrapper != null) {
                try {
                    genericRowData.setField(pos, toInternalConverters[pos].deserialize(valueWrapper));
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            } else {
                genericRowData.setField(pos, null);
            }
        }
        return genericRowData;
    }

    public Row toExternal(RowData rowData) throws SQLException {
        Row row = new Row(rowData.getArity());
        for(int i = 0; i < rowData.getArity(); i++) {
            if (!rowData.isNullAt(i)) {
                toExternalConverters[i].serialize(rowData, i, row);
            } else {
               row.setField(i, null);
            }
        }
        return row;
    }

    /**
     * Runtime converter to convert Nebula BaseTableRow to {@link RowData} type object.
     */
    @FunctionalInterface
    interface NebulaDeserializationConverter extends Serializable {
        /**
         * Convert a Nebula DataStructure of {@link BaseTableRow} to the internal data structure object.
         *
         * @param baseTableRow
         */
        Object deserialize(ValueWrapper baseTableRow) throws SQLException, UnsupportedEncodingException;
    }

    @FunctionalInterface
    interface NebulaSerializationConverter extends Serializable {
        /**
         * Convert a internal field to java object and fill into the Row.
         */
        void serialize(RowData rowData, int index, Row row) throws SQLException;
    }

    private NebulaDeserializationConverter createInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return val -> null;
            case BOOLEAN:
                return ValueWrapper::asBoolean;
            case TINYINT:
            case SMALLINT:
            case INTEGER:
                return val -> (int) val.asLong();
            case BIGINT:
                return ValueWrapper::asLong;
            case FLOAT:
            case DOUBLE:
                return ValueWrapper::asDouble;
            case CHAR:
            case VARCHAR:
                return val -> val.isGeography() ? StringData.fromString(
                        val.asGeography().toString())
                        : StringData.fromString(val.asString());
            case DATE:
                return val -> {
                    DateWrapper dateWrapper = val.asDate();
                    Date date = Date.valueOf(dateWrapper.toString());
                    return (int) date.toLocalDate().toEpochDay();
                };
            case TIME_WITHOUT_TIME_ZONE:
                return val -> {
                    TimeWrapper t = val.asTime();
                    LocalTime localTime = LocalTime.of(
                            t.getHour(), t.getMinute(), t.getSecond());
                    Time time = Time.valueOf(localTime);
                    return (int)(time.toLocalTime().toNanoOfDay() / 1_000_000L);
                };
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return val -> {
                    if (val.isDateTime()) {
                        DateTimeWrapper t = val.asDateTime();
                        LocalDateTime localDateTime = LocalDateTime.of(t.getYear(), t.getMonth(), t.getDay(),
                                t.getHour(), t.getMinute(), t.getSecond());
                        return TimestampData.fromLocalDateTime(localDateTime);
                    } else {
                        return TimestampData.fromTimestamp(new Timestamp(val.asLong() * 1000));
                    }
                };
            case BINARY:
            case ARRAY:
            case ROW:
            case MAP:
            case MULTISET:
            case RAW:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
            case VARBINARY:
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    private NebulaSerializationConverter createExternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return (val, idx, row) -> row.setField(idx, val.getBoolean(idx));
            case TINYINT:
                return (val, idx, row) -> row.setField(idx, val.getByte(idx));
            case SMALLINT:
                return (val, idx, row) -> row.setField(idx, val.getShort(idx));
            case INTEGER:
                return (val, idx, row) -> row.setField(idx, val.getInt(idx));
            case BIGINT:
                return (val, idx, row) -> row.setField(idx, val.getLong(idx));
            case FLOAT:
                return (val, idx, row) -> row.setField(idx, val.getFloat(idx));
            case DOUBLE:
                return (val, idx, row) -> row.setField(idx, val.getDouble(idx));
            case CHAR:
            case VARCHAR:
                return (val, idx, row) -> row.setField(idx, val.getString(idx).toString());
            case DATE:
                return (val, idx, row) -> row.setField(idx,
                        Date.valueOf(LocalDate.ofEpochDay(val.getInt(idx))));
            case TIME_WITHOUT_TIME_ZONE:
                return (val, idx, row) -> {
                    LocalTime localTime = LocalTime.ofNanoOfDay(val.getInt(idx) * 1_000_000L);
                    row.setField(idx, localTime.toString());
                };
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                final int timeStampPrecision =
                        ((TimestampType) type).getPrecision();
                return (val, idx, row) -> {
                    row.setField(idx, Timestamp.from(
                            val.getTimestamp(idx, timeStampPrecision).toInstant()));
                };
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int localZonedTimeStampPrecision =
                        ((LocalZonedTimestampType) type).getPrecision();
                return (val, idx, row) -> {
                    row.setField(idx, Timestamp.from(
                            val.getTimestamp(idx, localZonedTimeStampPrecision).toInstant()));
                };
            case BINARY:
            case ARRAY:
            case ROW:
            case MAP:
            case MULTISET:
            case RAW:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
            case VARBINARY:
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }
}
