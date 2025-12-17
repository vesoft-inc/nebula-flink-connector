/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.utils;

import com.vesoft.nebula.driver.graph.data.HostAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NebulaUtils {
    private static final String vectorType     = "VECTOR<";
    private static final String listType       = "LIST<";
    private static final String listStringType = "LIST<STRING";
    private static final String setType        = "SET<";
    private static final String setStringType  = "SET<STRING";
    private static final String mapType        = "MAP<";
    private static final String geoType        = "GEOGRAPHY";

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


    public static String extractPropertyValue(Map<String, String> schema,
                                              String propName,
                                              String value,
                                              String nullValue) {
        return extractValue(schema.get(propName), value, nullValue);
    }

    public static String extractValue(String dataType, String value, String nullValue) {
        if (value == null || value.equals(nullValue)) {
            return null;
        }
        if (dataType.equals("STRING")) {
            return mkString(escape(value), "\"", "", "\"");
        }
        if (value.isEmpty()) {
            return null;
        }
        // process the list type
        if (dataType.startsWith(listType)) {
            if (dataType.startsWith(listStringType)) {
                StringBuilder sb = new StringBuilder();
                sb.append("LIST[");
                String  trimmedInput = value.replaceAll("^\\[|\\]$", "");
                Pattern pattern      = Pattern.compile("(['\"])((?:\\\\\\1|.)*?)\\1|([^,]+)");
                Matcher matcher      = pattern.matcher(trimmedInput);

                while (matcher.find()) {
                    if (matcher.group(1) != null) {
                        String ele = matcher.group(2)
                                .replace("\\" + matcher.group(1), matcher.group(1));
                        sb.append("\"")
                                .append(escape(ele))
                                .append("\"").append(",");
                    } else {
                        sb.append("\"")
                                .append(escape(matcher.group(3)))
                                .append("\"").append(",");
                    }
                }

                if (sb.length() > 5) {
                    sb.deleteCharAt(sb.length() - 1);
                }
                sb.append("]");
                return sb.toString();
            } else {
                return "LIST" + value;
            }
        }
        // process the list type
        if (dataType.startsWith(setType)) {
            if (dataType.startsWith(setStringType)) {
                StringBuilder sb = new StringBuilder();
                sb.append("SET{");
                String  trimmedInput = value.replaceAll("^\\{|\\}$", "");
                Pattern pattern      = Pattern.compile("(['\"])((?:\\\\\\1|.)*?)\\1|([^,]+)");
                Matcher matcher      = pattern.matcher(trimmedInput);

                while (matcher.find()) {
                    if (matcher.group(1) != null) {
                        String ele = matcher.group(2)
                                .replace("\\" + matcher.group(1), matcher.group(1));
                        sb.append("\"")
                                .append(escape(ele))
                                .append("\"").append(",");
                    } else {
                        sb.append("\"")
                                .append(escape(matcher.group(3)))
                                .append("\"").append(",");
                    }
                }

                if (sb.length() > 5) {
                    sb.deleteCharAt(sb.length() - 1);
                }
                sb.append("}");
                return sb.toString();
            } else {
                return "SET" + value;
            }
        }
        // process the map type
        if (dataType.startsWith(mapType)) {
            StringBuilder sb = new StringBuilder();
            sb.append("MAP").append(value);
            return sb.toString();
        }
        // process the vector type
        if (dataType.startsWith(vectorType)) {
            StringBuilder sb = new StringBuilder();
            sb.append(dataType).append("(").append(value).append(")");
            return sb.toString();
        }
        if (dataType.startsWith(geoType)) {
            StringBuilder sb = new StringBuilder();
            sb.append("ST_GeogFromText(\"").append(value).append("\")");
            return sb.toString();
        }
        // process other data type
        switch (dataType) {
            case "DATE":
                return "date(\"" + value + "\")";
            case "LOCAL DATETIME":
                return "local_datetime(\"" + value + "\")";
            case "LOCAL TIME": {
                return "local_time(\"" + value + "\")";
            }
            case "ZONED DATETIME":
                return "zoned_datetime(\"" + value + "\")";
            case "ZONED TIME":
                return "zoned_time(\"" + value + "\")";
            case "DURATION": {
                return "duration(\"" + value + "\")";
            }
            default:
                return value;
        }

    }

    public static String mkString(String value, String start, String sep, String end) {
        StringBuilder builder = new StringBuilder();
        boolean       first   = true;
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


    public static String escape(String value) {
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

}
