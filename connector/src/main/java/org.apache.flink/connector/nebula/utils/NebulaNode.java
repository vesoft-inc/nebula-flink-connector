/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.utils;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class NebulaNode implements Serializable {


    private Map<String, String> properties;

    public NebulaNode(Map<String, String> properties) {
        this.properties = properties;
    }


    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    public String toString() {
        return "NebulaNode{"
                + "properties=" + properties
                + '}';
    }
}
