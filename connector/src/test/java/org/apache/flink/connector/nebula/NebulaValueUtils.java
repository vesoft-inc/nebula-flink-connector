/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula;

import static com.vesoft.nebula.client.graph.net.Session.value2Nvalue;

import com.vesoft.nebula.Coordinate;
import com.vesoft.nebula.Date;
import com.vesoft.nebula.DateTime;
import com.vesoft.nebula.Geography;
import com.vesoft.nebula.Point;
import com.vesoft.nebula.Row;
import com.vesoft.nebula.Time;
import com.vesoft.nebula.Value;
import java.util.Arrays;

public class NebulaValueUtils {
    public static Row rowOf(Value... values) {
        return new Row(Arrays.asList(values));
    }

    public static Value valueOf(Object obj) {
        return value2Nvalue(obj);
    }

    public static Value dateOf(int year, int month, int day) {
        return value2Nvalue(new Date((short) year, (byte) month, (byte) day));
    }

    public static Value dateTimeOf(int year, int month, int day,
                            int hour, int minute, int sec, int microsec) {
        return value2Nvalue(new DateTime(
                (short) year, (byte) month, (byte) day,
                (byte) hour, (byte) minute, (byte) sec, microsec));
    }

    public static Value timeOf(int hour, int minute, int sec, int microsec) {
        return value2Nvalue(new Time((byte) hour, (byte) minute, (byte) sec, microsec));
    }

    public static Value pointOf(double x, double y) {
        return value2Nvalue(new Geography(Geography.PTVAL, new Point(new Coordinate(x, y))));
    }
}
