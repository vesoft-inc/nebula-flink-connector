/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.utils;

import java.io.Serializable;
import java.util.Map;
import java.util.stream.Collectors;

public class NebulaGraph implements Serializable {

    private String              graphName;
    private Map<String, String> props;
    String graphTypeName = null;

    public NebulaGraph() {
    }

    public NebulaGraph(String graphName, Map<String, String> props) {
        this.graphName = graphName;
        this.props = props;
    }

    public String getGraphName() {
        return graphName;
    }

    public void setGraphName(String graphName) {
        this.graphName = graphName;
    }

    public Map<String, String> getProps() {
        return props;
    }

    public void setProps(Map<String, String> props) {
        this.props = props;
    }

    public String getCreateGraphType(boolean ignoreIfExist) {
        getGraphTypeName();
        String ignore = ignoreIfExist ? " IF NOT EXISTS " : "";
        return "CREATE GRAPH TYPE " + ignore + graphTypeName + " AS{}";
    }

    public String getCreateGraph(boolean ignoreIfExist) {
        String ignore = ignoreIfExist ? " IF NOT EXISTS " : "";
        getGraphTypeName();
        return "CREATE GRAPH " + ignore + graphName + " TYPED " + graphTypeName;
    }

    private void getGraphTypeName() {
        if (graphTypeName != null) {
            return;
        }
        for (String key : props.keySet()) {
            if (key.equalsIgnoreCase("graph_type")) {
                graphTypeName = props.get(key);
            }
        }
    }
}
