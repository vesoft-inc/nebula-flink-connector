/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.utils;

import com.vesoft.nebula.client.graph.data.HostAddress;
import com.vesoft.nebula.meta.PropertyType;
import java.util.ArrayList;
import java.util.List;

public class NebulaUtils {

    public static List<HostAddress> getHostAndPorts(String address) {
        if (address == null || "".equalsIgnoreCase(address)) {
            throw new IllegalArgumentException("empty address");
        }
        List<HostAddress> hostAndPortList = new ArrayList<>();
        for (String addr : address.split(NebulaConstant.COMMA)) {
            String[] hostPort = addr.split(NebulaConstant.COLON);
            if (hostPort.length < 2) {
                throw new IllegalArgumentException("wrong address");
            }
            hostAndPortList.add(new HostAddress(hostPort[0], Integer.parseInt(hostPort[1])));
        }
        return hostAndPortList;
    }

    public static boolean isNumeric(String str) {
        String newStr = null;
        if (str.startsWith("-")) {
            newStr = str.substring(1);
        } else {
            newStr = str;
        }
        for (char c : newStr.toCharArray()) {
            if (!Character.isDigit(c)) {
                return false;
            }
        }
        return true;
    }


    public static String extraValue(Object value, int type) {
        if (value == null) {
            return null;
        }
        switch (PropertyType.findByValue(type)) {
            case STRING:
            case FIXED_STRING:
                return mkString(escapeUtil(String.valueOf(value)), "\"", "", "\"");
            case DATE:
                return "date(\"" + value + "\")";
            case TIME:
                return "time(\"" + value + "\")";
            case DATETIME:
                return "datetime(\"" + value + "\")";
            case TIMESTAMP: {
                if (isNumeric(String.valueOf(value))) {
                    return String.valueOf(value);
                } else {
                    return "timestamp(\"" + value + "\")";
                }
            }
            case GEOGRAPHY:
                return "ST_GeogFromText(\"" + value + "\")";
            default: {
                return String.valueOf(value);
            }

        }
    }


    public static String escapeUtil(String value) {
        String s = value;
        if (s.contains("\\")) {
            s = s.replaceAll("\\\\", "\\\\\\\\");
        }
        if (s.contains("\t")) {
            s = s.replaceAll("\t", "\\\\t");
        }
        if (s.contains("\n")) {
            s = s.replaceAll("\n", "\\\\n");
        }
        if (s.contains("\"")) {
            s = s.replaceAll("\"", "\\\\\"");
        }
        if (s.contains("\'")) {
            s = s.replaceAll("\'", "\\\\'");
        }
        if (s.contains("\r")) {
            s = s.replaceAll("\r", "\\\\r");
        }
        if (s.contains("\b")) {
            s = s.replaceAll("\b", "\\\\b");
        }
        return s;
    }

    public static String mkString(String value, String start, String sep, String end) {
        StringBuilder builder = new StringBuilder();
        boolean first = true;
        builder.append(start);
        for (char c : value.toCharArray()) {
            if (first) {
                builder.append(c);
                first = false;
            } else {
                builder.append(sep);
                builder.append(c);
            }
        }
        builder.append(end);
        return builder.toString();
    }
}
