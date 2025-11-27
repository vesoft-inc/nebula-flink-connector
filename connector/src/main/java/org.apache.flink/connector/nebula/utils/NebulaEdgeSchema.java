/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NebulaEdgeSchema {
    private String edgeTypeName;
    private String srcNodeTypeName;
    private String dstNodeTypeName;

    private List<String>       srcPkNames       = new ArrayList<>();
    private Map<String,String> srcPkDataTypeMap = new HashMap<>();
    private List<String>       dstPkNames       = new ArrayList<>();
    private Map<String, String> dstPkDataTypeMap = new HashMap<>();

    private List<String> propNames = new ArrayList<>();
    private Map<String,String> properties = new HashMap<>();

    public String getEdgeTypeName() {
        return edgeTypeName;
    }

    public void setEdgeTypeName(String edgeTypeName) {
        this.edgeTypeName = edgeTypeName;
    }

    public String getSrcNodeTypeName() {
        return srcNodeTypeName;
    }

    public void setSrcNodeTypeName(String srcNodeTypeName) {
        this.srcNodeTypeName = srcNodeTypeName;
    }

    public String getDstNodeTypeName() {
        return dstNodeTypeName;
    }

    public void setDstNodeTypeName(String dstNodeTypeName) {
        this.dstNodeTypeName = dstNodeTypeName;
    }

    public List<String> getSrcPkNames() {
        return srcPkNames;
    }

    public void setSrcPkNames(List<String> srcPkNames) {
        this.srcPkNames = srcPkNames;
    }

    public Map<String, String> getSrcPkDataTypeMap() {
        return srcPkDataTypeMap;
    }

    public void setSrcPkDataTypeMap(Map<String, String> srcPkDataTypeMap) {
        this.srcPkDataTypeMap = srcPkDataTypeMap;
    }

    public List<String> getDstPkNames() {
        return dstPkNames;
    }

    public void setDstPkNames(List<String> dstPkNames) {
        this.dstPkNames = dstPkNames;
    }

    public Map<String, String> getDstPkDataTypeMap() {
        return dstPkDataTypeMap;
    }

    public void setDstPkDataTypeMap(Map<String, String> dstPkDataTypeMap) {
        this.dstPkDataTypeMap = dstPkDataTypeMap;
    }

    public List<String> getPropNames() {
        return propNames;
    }

    public void setPropNames(List<String> propNames) {
        this.propNames = propNames;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }
}
