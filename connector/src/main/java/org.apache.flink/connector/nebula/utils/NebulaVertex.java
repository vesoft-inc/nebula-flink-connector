/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package org.apache.flink.connector.nebula.utils;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

public class NebulaVertices implements Serializable {

    private String vid;
    private List<String> propValues;

    public String getVid() {
        return vid;
    }

    public void setVid(String vid) {
        this.vid = vid;
    }

    public String getPropValues() {
        return propValues.stream().collect(Collectors.joining(","));
    }

    public void setPropValues(List<String> propValues) {
        this.propValues = propValues;
    }
}
