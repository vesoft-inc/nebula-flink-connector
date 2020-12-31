/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
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
            hostAndPortList.add(new HostAddress(hostPort[0], Integer.parseInt(hostPort[1])));
        }
        return hostAndPortList;
    }

    public static boolean isNumeric(String str) {
        for (char c : str.toCharArray()) {
            if (!Character.isDigit(c)) {
                return false;
            }
        }
        return true;
    }


    public static String extraValue(Object value, int type) {
        switch (type) {
            case PropertyType.STRING:
            case PropertyType.FIXED_STRING:
                return mkString(escapeUtil(String.valueOf(value)), "\"", "", "\"");
            case PropertyType.DATE:
                return "date(\"" + value + "\")";
            case PropertyType.TIME:
                return "time(\"" + value + "\")";
            case PropertyType.DATETIME:
                return "datetime(\"" + value + "\")";
            default:
                return String.valueOf(value);
        }
    }


    private static String escapeUtil(String value) {
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
            if (first = true) {
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
