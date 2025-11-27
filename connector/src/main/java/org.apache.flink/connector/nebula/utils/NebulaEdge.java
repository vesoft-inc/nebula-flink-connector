/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.utils;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class NebulaEdge implements Serializable {

    private Map<String,String>  srcPks;
    private Map<String, String> dstPks;
    private Map<String, String> properties;

    public NebulaEdge(Map<String, String> srcPks, Map<String, String> dstPks,
                      Map<String, String> properties) {
        this.srcPks = srcPks;
        this.dstPks = dstPks;
        this.properties = properties;
    }

    public Map<String, String> getSrcPks() {
        return srcPks;
    }

    public void setSrcPks(Map<String, String> srcPks) {
        this.srcPks = srcPks;
    }

    public Map<String, String> getDstPks() {
        return dstPks;
    }

    public void setDstPks(Map<String, String> dstPks) {
        this.dstPks = dstPks;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }


    @Override
    public String toString() {
        return "NebulaEdge{"
                + "srcPks=" + srcPks
                + ", dstPks=" + dstPks
                + ", properties=" + properties
                + '}';
    }
}
