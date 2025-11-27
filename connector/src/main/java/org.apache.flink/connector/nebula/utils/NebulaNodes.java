/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class NebulaNodes implements Serializable {

    private static final String           NODE_ALIAS = "n_v";
    private              NebulaNodeSchema nodeSchema;
    private              List<NebulaNode> nodes;

    public NebulaNodes(NebulaNodeSchema nodeSchema, List<NebulaNode> nodes) {
        this.nodeSchema = nodeSchema;
        this.nodes = nodes;
    }

    public String getPropNames() {
        List<String> escapePropNames = new ArrayList<>();
        for (String propName : nodeSchema.getPropNames()) {
            escapePropNames.add(NebulaUtils.mkString(propName, "`", "", "`"));
        }
        return String.join(",", escapePropNames);
    }

    public NebulaNodeSchema getNodeSchema() {
        return nodeSchema;
    }

    public void setNodeSchema(NebulaNodeSchema nodeSchema) {
        this.nodeSchema = nodeSchema;
    }

    public List<NebulaNode> getNodes() {
        return nodes;
    }

    public void setNodes(List<NebulaNode> nodes) {
        this.nodes = nodes;
    }

    /**
     * construct Nebula batch insert ngql for nodes
     *
     * @return ngql
     */
    public String getInsertStatement(String graphName,
                                     WriteModeEnum insertMode,
                                     List<String> flinkFields,
                                     List<String> nebulaFields) {
        if (nodes.isEmpty()) {
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
        String format = "TABLE t{%s} = \n"
                + "%s \n"
                + "USE `%s` \n"
                + "FOR r IN t \n"
                + "%s (@`%s`{%s})";

        return String.format(format,
                             getTableHeaders(flinkFields),
                             getTableValues(nebulaFields),
                             graphName,
                             insertModeString,
                             nodeSchema.getNodeTypeName(),
                             getProperties(nebulaFields));
    }

    /**
     * construct Nebula batch update ngql for vertex
     *
     * @return ngql
     */
    public String getUpdateStatement(String graphName,
                                     List<String> flinkFields,
                                     List<String> nebulaFields) {
        if (nodes.isEmpty()) {
            return null;
        }
        String format = "TABLE t{%s} = \n"
                + "%s \n"
                + "USE `%s` \n"
                + "FOR r IN t \n"
                + "OPTIONAL MATCH (%s@`%s`) WHERE %s \n"
                + "SET %s";
        return String.format(format,
                             getTableHeaders(flinkFields),
                             getTableValues(nebulaFields),
                             graphName,
                             NODE_ALIAS,
                             nodeSchema.getNodeTypeName(),
                             getPkFilters(nebulaFields),
                             getUpdateProperties(nebulaFields));
    }


    /**
     * construct Nebula batch delete ngql for nodes
     *
     * @return ngql
     */
    public String getDeleteStatement(String graphName,
                                     WriteModeEnum deleteMode,
                                     List<String> flinkFields,
                                     List<String> nebulaFields) {
        if (nodes.isEmpty()) {
            return null;
        }
        String deleteModeString;
        switch (deleteMode) {
            case DELETE:
                deleteModeString = "DELETE";
                break;
            case DETACHDELETE:
                deleteModeString = "DETACH DELETE";
                break;
            default:
                throw new IllegalArgumentException("insert mode is illegal for delete:"
                                                           + deleteMode);
        }
        String format = "TABLE t{%s} = \n"
                + "%s \n"
                + "USE `%s` \n"
                + "FOR r IN t \n"
                + "OPTIONAL MATCH (%s@`%s`) WHERE %s \n"
                + "%s %s";
        return String.format(format,
                             getDeleteTableHeaders(nebulaFields),
                             getDeleteTableValues(nebulaFields),
                             graphName,
                             NODE_ALIAS,
                             nodeSchema.getNodeTypeName(),
                             getPkFilters(nebulaFields),
                             deleteModeString,
                             NODE_ALIAS);
    }


    private String getTableHeaders(List<String> flinkFields) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < flinkFields.size(); i++) {
            if (i > 0) {
                sb.append(",");
            }
            sb.append('c').append(i);
        }
        return sb.toString();
    }

    private String getDeleteTableHeaders(List<String> nebulaFields) {
        StringBuilder sb = new StringBuilder();
        for (String pk : nodeSchema.getPkNames()) {
            if (sb.length() > 0) {
                sb.append(",");
            }
            sb.append('c').append(nebulaFields.indexOf(pk));
        }
        return sb.toString();

    }

    private String getTableValues(List<String> nebulaFields) {
        List<String> tableRows = new ArrayList<>();
        for (NebulaNode node : nodes) {
            List<String>  rowValues      = new ArrayList<>();
            StringBuilder rowValueString = new StringBuilder();
            for (String propName : nebulaFields) {
                rowValues.add(node.getProperties().get(propName));
            }
            rowValueString.append("(").append(String.join(",", rowValues)).append(")");
            tableRows.add(rowValueString.toString());
        }
        return String.join(",", tableRows);
    }

    private String getDeleteTableValues(List<String> nebulaFields) {
        List<String> tableRows = new ArrayList<>();
        for (NebulaNode node : nodes) {
            List<String>  rowValues      = new ArrayList<>();
            StringBuilder rowValueString = new StringBuilder();
            for (String pk : nodeSchema.getPkNames()) {
                rowValues.add(node.getProperties().get(pk));
            }
            rowValueString.append("(").append(String.join(",", rowValues)).append(")");
            tableRows.add(rowValueString.toString());
        }
        return String.join(",", tableRows);
    }

    private String getProperties(List<String> nebulaFields) {
        StringBuilder propertyString = new StringBuilder();
        for (String propertyName : nebulaFields) {
            propertyString.append('`').append(propertyName).append("`:r.c")
                    .append(nebulaFields.indexOf(propertyName)).append(",");
        }
        if (propertyString.length() > 0) {
            propertyString.deleteCharAt(propertyString.length() - 1);
        }
        return propertyString.toString();
    }


    private String getUpdateProperties(List<String> nebulaFields) {
        StringBuilder propertyString = new StringBuilder();
        for (int i = 0; i < nebulaFields.size(); i++) {
            if (nodeSchema.getPkNames().contains(nebulaFields.get(i))) {
                continue;
            }
            propertyString
                    .append(NODE_ALIAS)
                    .append(".")
                    .append(nebulaFields.get(i))
                    .append("=r.c")
                    .append(i)
                    .append(",");
        }
        if (propertyString.length() > 0) {
            propertyString.deleteCharAt(propertyString.length() - 1);
        }
        return propertyString.toString();
    }

    private String getPkFilters(List<String> nebulaFields) {
        StringBuilder pkFilterString = new StringBuilder();
        for (String pk : nodeSchema.getPkNames()) {
            pkFilterString
                    .append(NODE_ALIAS)
                    .append(".`")
                    .append(pk)
                    .append("`=r.c")
                    .append(nebulaFields.indexOf(pk))
                    .append(",");
        }
        if (pkFilterString.length() > 0) {
            pkFilterString.deleteCharAt(pkFilterString.length() - 1);
        }
        return pkFilterString.toString();
    }
}
