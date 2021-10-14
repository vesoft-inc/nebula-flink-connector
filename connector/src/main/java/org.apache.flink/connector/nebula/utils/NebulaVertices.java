/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package org.apache.flink.connector.nebula.utils;

import static org.apache.flink.connector.nebula.utils.NebulaConstant.BATCH_INSERT_TEMPLATE;
import static org.apache.flink.connector.nebula.utils.NebulaConstant.DELETE_VERTEX_TEMPLATE;
import static org.apache.flink.connector.nebula.utils.NebulaConstant.ENDPOINT_TEMPLATE;
import static org.apache.flink.connector.nebula.utils.NebulaConstant.UPDATE_VALUE_TEMPLATE;
import static org.apache.flink.connector.nebula.utils.NebulaConstant.UPDATE_VERTEX_TEMPLATE;
import static org.apache.flink.connector.nebula.utils.NebulaConstant.VERTEX_VALUE_TEMPLATE;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class NebulaVertices implements Serializable {

    private String tagName;
    private List<String> propNames;
    private List<NebulaVertex> vertices;
    private PolicyEnum policy = null;

    public NebulaVertices(String tagName, List<String> propNames, List<NebulaVertex> vertices,
                          PolicyEnum policy) {
        this.tagName = tagName;
        this.propNames = propNames;
        this.vertices = vertices;
        this.policy = policy;
    }

    public String getPropNames() {
        List<String> escapePropNames = new ArrayList<>();
        for (String propName : propNames) {
            escapePropNames.add(NebulaUtils.mkString(propName, "`", "", "`"));
        }
        return String.join(",", escapePropNames);
    }

    public void setPropNames(List<String> propNames) {
        this.propNames = propNames;
    }

    public List<NebulaVertex> getVertices() {
        return vertices;
    }

    public void setVertices(List<NebulaVertex> vertices) {
        this.vertices = vertices;
    }

    public PolicyEnum getPolicy() {
        return policy;
    }

    public void setPolicy(PolicyEnum policy) {
        this.policy = policy;
    }

    /**
     * construct Nebula batch insert ngql for vertex
     *
     * @return ngql
     */
    public String getInsertStatement() {
        List<String> values = new ArrayList<>();
        for (NebulaVertex vertex : vertices) {
            String vertexId = getVertexId(vertex);
            values.add(String.format(VERTEX_VALUE_TEMPLATE, vertexId,
                    vertex.getPropValuesString()));
        }
        return String.format(BATCH_INSERT_TEMPLATE, DataTypeEnum.VERTEX.name(), tagName,
                getPropNames(), String.join(",", values));
    }

    /**
     * construct Nebula batch update ngql for vertex
     *
     * @return ngql
     */
    public String getUpdateStatement() {
        List<String> statements = new ArrayList<>();
        // for update mode, each vertex construct one update statement.
        for (NebulaVertex vertex : vertices) {
            String vertexId = getVertexId(vertex);

            List<String> updateProps = new ArrayList<>();
            for (int i = 0; i < propNames.size(); i++) {
                updateProps.add(String.format(UPDATE_VALUE_TEMPLATE, propNames.get(i),
                        vertex.getPropValues().get(i)));
            }
            String updatePropsString = String.join(",", updateProps);
            String statement = String.format(UPDATE_VERTEX_TEMPLATE, DataTypeEnum.VERTEX.name(),
                    tagName, vertexId, updatePropsString);
            statements.add(statement);
        }
        return String.join(";", statements);
    }

    /**
     * construct Nebula batch delete ngql for vertex
     *
     * @return ngql
     */
    public String getDeleteStatement() {
        List<String> vertexIds = new ArrayList<>();
        for (NebulaVertex vertex : vertices) {
            String vertexId = getVertexId(vertex);
            vertexIds.add(vertexId);
        }
        return String.format(DELETE_VERTEX_TEMPLATE, String.join(",", vertexIds));
    }

    /**
     * format vertex id with policy
     */
    private String getVertexId(NebulaVertex vertex) {
        String vertexId = null;
        if (policy == null) {
            vertexId = vertex.getVid();
        } else {
            switch (policy) {
                case HASH:
                    vertexId = String.format(ENDPOINT_TEMPLATE, PolicyEnum.HASH.name(),
                            vertex.getVid());
                    break;
                case UUID:
                    vertexId = String.format(ENDPOINT_TEMPLATE, PolicyEnum.UUID.name(),
                            vertex.getVid());
                    break;
                default:
                    throw new IllegalArgumentException("policy is not supported");
            }
        }
        return vertexId;
    }
}
