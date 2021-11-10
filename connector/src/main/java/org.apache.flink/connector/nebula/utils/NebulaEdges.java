/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.utils;

import static org.apache.flink.connector.nebula.utils.NebulaConstant.BATCH_INSERT_TEMPLATE;
import static org.apache.flink.connector.nebula.utils.NebulaConstant.DELETE_EDGE_TEMPLATE;
import static org.apache.flink.connector.nebula.utils.NebulaConstant.EDGE_ENDPOINT_TEMPLATE;
import static org.apache.flink.connector.nebula.utils.NebulaConstant.EDGE_VALUE_TEMPLATE;
import static org.apache.flink.connector.nebula.utils.NebulaConstant.EDGE_VALUE_WITHOUT_RANKING_TEMPLATE;
import static org.apache.flink.connector.nebula.utils.NebulaConstant.ENDPOINT_TEMPLATE;
import static org.apache.flink.connector.nebula.utils.NebulaConstant.UPDATE_EDGE_TEMPLATE;
import static org.apache.flink.connector.nebula.utils.NebulaConstant.UPDATE_VALUE_TEMPLATE;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class NebulaEdges implements Serializable {

    private String edgeType;
    private List<String> propNames;
    private List<NebulaEdge> edges;
    private PolicyEnum sourcePolicy = null;
    private PolicyEnum targetPolicy = null;

    public NebulaEdges(String edgeType, List<String> propNames, List<NebulaEdge> edges,
                       PolicyEnum sourcePolicy, PolicyEnum targetPolicy) {
        this.edgeType = edgeType;
        this.propNames = propNames;
        this.edges = edges;
        this.sourcePolicy = sourcePolicy;
        this.targetPolicy = targetPolicy;
    }

    public String getEdgeType() {
        return edgeType;
    }

    public String getPropNames() {
        List<String> escapePropNames = new ArrayList<>();
        for (String propName : propNames) {
            escapePropNames.add(NebulaUtils.mkString(propName, "`", "", "`"));
        }
        return String.join(",", escapePropNames);
    }

    public List<NebulaEdge> getEdges() {
        return edges;
    }

    public PolicyEnum getSourcePolicy() {
        return sourcePolicy;
    }

    public PolicyEnum getTargetPolicy() {
        return targetPolicy;
    }

    /**
     * construct Nebula batch insert ngql for edges
     *
     * @return ngql
     */
    public String getInsertStatement() {
        List<String> values = new ArrayList<>();
        for (NebulaEdge edge : edges) {
            String sourceId = getSourceId(edge);
            String targetId = getTargetId(edge);

            // edge rank
            if (edge.getRank() == null) {
                values.add(String.format(EDGE_VALUE_WITHOUT_RANKING_TEMPLATE, sourceId, targetId,
                        edge.getPropValuesString()));
            } else {
                values.add(String.format(EDGE_VALUE_TEMPLATE, sourceId, targetId, edge.getRank(),
                        edge.getPropValuesString()));
            }
        }
        return String.format(BATCH_INSERT_TEMPLATE, DataTypeEnum.EDGE.name(), edgeType,
                getPropNames(), String.join(",", values));
    }

    /**
     * construct Nebula batch update ngql for edge
     *
     * @return ngql
     */
    public String getUpdateStatement() {
        List<String> statements = new ArrayList<>();
        // for update mode, each vertex construct one update statement.
        for (NebulaEdge edge : edges) {
            String sourceId = getSourceId(edge);
            String targetId = getTargetId(edge);
            long rank = 0;
            if (edge.getRank() != null) {
                rank = edge.getRank();
            }

            List<String> updateProps = new ArrayList<>();
            for (int i = 0; i < propNames.size(); i++) {
                updateProps.add(String.format(UPDATE_VALUE_TEMPLATE, propNames.get(i),
                        edge.getPropValues().get(i)));
            }
            String updatePropsString = String.join(",", updateProps);
            String statement = String.format(UPDATE_EDGE_TEMPLATE, DataTypeEnum.EDGE.name(),
                    edgeType, sourceId, targetId, rank, updatePropsString);
            statements.add(statement);
        }
        return String.join(";", statements);
    }

    /**
     * construct Nebula batch delete ngql for edge
     *
     * @return ngql
     */
    public String getDeleteStatement() {
        List<String> sourceTargetIds = new ArrayList<>();
        for (NebulaEdge edge : edges) {
            String sourceId = getSourceId(edge);
            String targetId = getTargetId(edge);
            long rank = 0;
            if (edge.getRank() != null) {
                rank = edge.getRank();
            }
            String statement = String.format(EDGE_ENDPOINT_TEMPLATE, sourceId, targetId, rank);
            sourceTargetIds.add(statement);
        }
        return String.format(DELETE_EDGE_TEMPLATE, edgeType, String.join(",", sourceTargetIds));
    }

    /**
     * format edge source id with policy
     *
     * @param edge Nebula edge {@link NebulaEdge}
     * @return the formatted source id
     */
    private String getSourceId(NebulaEdge edge) {
        String sourceId = null;
        if (sourcePolicy == null) {
            sourceId = edge.getSource();
        } else {
            switch (sourcePolicy) {
                case HASH:
                    sourceId = String.format(ENDPOINT_TEMPLATE, PolicyEnum.HASH.name(),
                            edge.getSource());
                    break;
                case UUID:
                    sourceId = String.format(ENDPOINT_TEMPLATE, PolicyEnum.UUID.name(),
                            edge.getSource());
                    break;
                default:
                    throw new IllegalArgumentException("source policy is not supported");
            }
        }
        return sourceId;
    }

    /**
     * format edge target id with policy
     *
     * @param edge Nebula edge {@link NebulaEdge}
     * @return the formatted target id
     */
    private String getTargetId(NebulaEdge edge) {
        String targetId = null;
        if (targetPolicy == null) {
            targetId = edge.getTarget();
        } else {
            switch (targetPolicy) {
                case HASH:
                    targetId = String.format(ENDPOINT_TEMPLATE, PolicyEnum.HASH.name(),
                            edge.getTarget());
                    break;
                case UUID:
                    targetId = String.format(ENDPOINT_TEMPLATE, PolicyEnum.UUID.name(),
                            edge.getTarget());
                    break;
                default:
                    throw new IllegalArgumentException("target policy is not supported");
            }
        }
        return targetId;
    }

}
