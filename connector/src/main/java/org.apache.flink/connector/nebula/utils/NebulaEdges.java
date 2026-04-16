/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class NebulaEdges implements Serializable {
    private static final String SRC_NODE_ALIAS = "n_src";
    private static final String DST_NODE_ALIAS = "n_dst";
    private static final String EDGE_ALIAS     = "n_e";

    private NebulaEdgeSchema edgeSchema;
    private List<NebulaEdge> edges;

    public NebulaEdges(NebulaEdgeSchema edgeSchema,
                       List<NebulaEdge> edges) {
        this.edgeSchema = edgeSchema;
        this.edges = edges;
    }

    public String getInsertStatement(String graphName,
                                     WriteModeEnum insertMode,
                                     List<String> flinkSrcFields,
                                     List<String> nebulaSrcPks,
                                     List<String> flinkDstFields,
                                     List<String> nebulaDstPks,
                                     List<String> flinkFields,
                                     List<String> nebulaFields) {
        if (edges.isEmpty()) {
            return null;
        }
        String insertModeString;
        switch (insertMode) {
            case INSERT:
                insertModeString = "INSERT";
                break;
            case INSERTIGNORE:
                insertModeString = "INSERT OR IGNORE";
                break;
            case INSERTREPLACE:
                insertModeString = "INSERT OR REPLACE";
                break;
            case INSERTUPDATE:
                insertModeString = "INSERT OR UPDATE";
                break;
            default:
                throw new IllegalArgumentException("insert mode is illegal for insert:"
                                                           + insertMode);
        }
        String format = "%s\n"
                + "USE `%s` \n"
                + "FOR r IN t \n"
                + "OPTIONAL MATCH (%s@`%s`) WHERE %s "
                + "OPTIONAL MATCH (%s@`%s`) WHERE %s \n"
                + "%s (%s)-[@`%s`{%s}]->(%s)";

        return String.format(format,
                             buildTableClause(flinkSrcFields,
                                              nebulaSrcPks,
                                              flinkDstFields,
                                              nebulaDstPks,
                                              flinkFields,
                                              nebulaFields),
                             graphName,
                             SRC_NODE_ALIAS,
                             edgeSchema.getSrcNodeTypeName(),
                             getSrcPkFilter(nebulaSrcPks),
                             DST_NODE_ALIAS,
                             edgeSchema.getDstNodeTypeName(),
                             getDstPkFilter(nebulaDstPks),
                             insertModeString,
                             SRC_NODE_ALIAS,
                             edgeSchema.getEdgeTypeName(),
                             getProperties(nebulaFields),
                             DST_NODE_ALIAS);
    }

    public String buildTableClause(List<String> flinkSrcFields,
                                   List<String> nebulaSrcPks,
                                   List<String> flinkDstFields,
                                   List<String> nebulaDstPks,
                                   List<String> flinkFields,
                                   List<String> nebulaFields) {
        return String.format("TABLE t{%s} = \n%s ",
                             getTableHeaders(flinkSrcFields, flinkDstFields, flinkFields),
                             getTableValues(nebulaSrcPks, nebulaDstPks, nebulaFields));
    }


    public String getUpdateStatement(String graphName,
                                     List<String> flinkSrcFields,
                                     List<String> nebulaSrcPks,
                                     List<String> flinkDstFields,
                                     List<String> nebulaDstPks,
                                     List<String> flinkFields,
                                     List<String> nebulaFields) {
        String format = "TABLE t{%s} = \n"
                + "%s \n"
                + "USE `%s` \n"
                + "FOR r IN t \n"
                + "OPTIONAL MATCH (%s@`%s`)-[%s@`%s`]->(%s@`%s`) "
                + "WHERE %s AND %s%s \n"
                + "SET %s";
        return String.format(format,
                             getTableHeaders(flinkSrcFields, flinkDstFields, flinkFields),
                             getTableValues(nebulaSrcPks, nebulaDstPks, nebulaFields),
                             graphName,
                             SRC_NODE_ALIAS,
                             edgeSchema.getSrcNodeTypeName(),
                             EDGE_ALIAS,
                             edgeSchema.getEdgeTypeName(),
                             DST_NODE_ALIAS,
                             edgeSchema.getDstNodeTypeName(),
                             getSrcPkFilter(nebulaSrcPks),
                             getDstPkFilter(nebulaDstPks),
                             getMultiEdgeKeysFilter(nebulaFields),
                             getUpdateProperties(nebulaFields));
    }

    public String getDeleteStatement(String graphName,
                                     List<String> flinkSrcFields,
                                     List<String> nebulaSrcPks,
                                     List<String> flinkDstFields,
                                     List<String> nebulaDstPks,
                                     List<String> nebulaFields) {
        String format = "TABLE t{%s} = \n"
                + "%s \n"
                + "USE `%s` \n"
                + "FOR r IN t \n"
                + "OPTIONAL MATCH (%s@`%s`)-[%s@`%s`]->(%s@`%s`) "
                + "WHERE %s AND %s%s \n"
                + "DELETE %s";
        return String.format(format,
                             getDeleteTableHeaders(flinkSrcFields, flinkDstFields, nebulaFields),
                             getDeleteTableValues(nebulaSrcPks, nebulaDstPks),
                             graphName,
                             SRC_NODE_ALIAS,
                             edgeSchema.getSrcNodeTypeName(),
                             EDGE_ALIAS,
                             edgeSchema.getEdgeTypeName(),
                             DST_NODE_ALIAS,
                             edgeSchema.getDstNodeTypeName(),
                             getSrcPkFilter(nebulaSrcPks),
                             getDstPkFilter(nebulaDstPks),
                             getMultiEdgeKeysFilter(nebulaFields),
                             EDGE_ALIAS);
    }

    private String getTableHeaders(List<String> flinkSrcFields,
                                   List<String> flinkDstFields,
                                   List<String> flinkFields) {
        List<String> headerNames = new ArrayList<>();
        for (int i = 0; i < flinkSrcFields.size(); i++) {
            headerNames.add("src_" + i);
        }
        for (int i = 0; i < flinkDstFields.size(); i++) {
            headerNames.add("dst_" + i);
        }
        for (int i = 0; i < flinkFields.size(); i++) {
            headerNames.add("c" + i);
        }
        return String.join(",", headerNames);
    }

    private String getTableValues(List<String> nebulaSrcPks,
                                  List<String> nebulaDstPks,
                                  List<String> nebulaFields) {
        List<String> tableRows = new ArrayList<>();
        for (NebulaEdge edge : edges) {
            List<String> rowValues = new ArrayList<>();
            for (int i = 0; i < nebulaSrcPks.size(); i++) {
                rowValues.add(edge.getSrcPks().get(nebulaSrcPks.get(i)));
            }
            for (int i = 0; i < nebulaDstPks.size(); i++) {
                rowValues.add(edge.getDstPks().get(nebulaDstPks.get(i)));
            }
            for (int i = 0; i < nebulaFields.size(); i++) {
                rowValues.add(edge.getProperties().get(nebulaFields.get(i)));
            }
            tableRows.add("(" + String.join(",", rowValues) + ")");
        }
        return String.join(",", tableRows);
    }

    private String getProperties(List<String> nebulaFields) {
        StringBuilder propertyString = new StringBuilder();
        for (int index = 0; index < nebulaFields.size(); index++) {
            propertyString
                    .append('`')
                    .append(nebulaFields.get(index))
                    .append("`:r.c")
                    .append(index)
                    .append(",");
        }
        if (propertyString.length() > 0) {
            propertyString.deleteCharAt(propertyString.length() - 1);
        }
        return propertyString.toString();
    }

    private String getDeleteTableHeaders(List<String> flinkSrcFields,
                                         List<String> flinkDstFields,
                                         List<String> nebulaFields) {
        List<String> headerNames = new ArrayList<>();
        for (int i = 0; i < flinkSrcFields.size(); i++) {
            headerNames.add("src_" + i);
        }
        for (int i = 0; i < flinkDstFields.size(); i++) {
            headerNames.add("dst_" + i);
        }
        for (String key : edgeSchema.getMultipleEdgeKeys()) {
            headerNames.add("c" + nebulaFields.indexOf(key));
        }
        return String.join(",", headerNames);
    }

    private String getDeleteTableValues(List<String> nebulaSrcPks, List<String> nebulaDstPks) {
        List<String> tableRows = new ArrayList<>();
        for (NebulaEdge edge : edges) {
            List<String> rowValues = new ArrayList<>();
            for (int i = 0; i < nebulaSrcPks.size(); i++) {
                rowValues.add(edge.getSrcPks().get(nebulaSrcPks.get(i)));
            }
            for (int i = 0; i < nebulaDstPks.size(); i++) {
                rowValues.add(edge.getDstPks().get(nebulaDstPks.get(i)));
            }
            for (String key : edgeSchema.getMultipleEdgeKeys()) {
                rowValues.add(edge.getProperties().get(key));
            }
            tableRows.add("(" + String.join(",", rowValues) + ")");
        }
        return String.join(",", tableRows);
    }

    private String getUpdateProperties(List<String> nebulaFields) {
        StringBuilder propertyString = new StringBuilder();
        for (int index = 0; index < nebulaFields.size(); index++) {
            if (edgeSchema.getMultipleEdgeKeys().contains(nebulaFields.get(index))) {
                continue;
            }
            propertyString
                    .append(EDGE_ALIAS)
                    .append(".`")
                    .append(nebulaFields.get(index))
                    .append("`=r.c")
                    .append(index)
                    .append(",");
        }
        if (propertyString.length() > 0) {
            propertyString.deleteCharAt(propertyString.length() - 1);
        }
        return propertyString.toString();
    }

    private String getMultiEdgeKeysFilter(List<String> nebulaFields) {
        if (edgeSchema.getMultipleEdgeKeys() == null
                || edgeSchema.getMultipleEdgeKeys().isEmpty()) {
            return "";
        }
        StringBuilder filter = new StringBuilder();
        for (String key : edgeSchema.getMultipleEdgeKeys()) {
            filter.append(" AND ")
                    .append(EDGE_ALIAS)
                    .append(".`")
                    .append(key)
                    .append("`=r.c")
                    .append(nebulaFields.indexOf(key));
        }
        return filter.toString();
    }

    private String getSrcPkFilter(List<String> nebulaSrcPks) {
        StringBuilder pkFilterString = new StringBuilder();
        for (int i = 0; i < nebulaSrcPks.size(); i++) {
            if (pkFilterString.length() > 0) {
                pkFilterString.append(" AND ");
            }
            pkFilterString.append(SRC_NODE_ALIAS)
                    .append(".`")
                    .append(nebulaSrcPks.get(i))
                    .append("`=r.src_")
                    .append(i);
        }
        return pkFilterString.toString();
    }


    private String getDstPkFilter(List<String> nebulaDstPks) {
        StringBuilder pkFilterString = new StringBuilder();
        for (int i = 0; i < nebulaDstPks.size(); i++) {
            if (pkFilterString.length() > 0) {
                pkFilterString.append(" AND ");
            }
            pkFilterString.append(DST_NODE_ALIAS)
                    .append(".`")
                    .append(nebulaDstPks.get(i))
                    .append("`=r.dst_")
                    .append(i);
        }
        return pkFilterString.toString();
    }
}
