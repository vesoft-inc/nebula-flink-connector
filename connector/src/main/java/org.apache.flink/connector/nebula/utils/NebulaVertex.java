/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.utils;

import java.io.Serializable;
import java.util.List;

public class NebulaVertex implements Serializable {

    private String vid;
    private List<String> propValues;

    public NebulaVertex(String vid, List<String> propValues) {
        this.vid = vid;
        this.propValues = propValues;
    }

    public String getVid() {
        return vid;
    }

    public void setVid(String vid) {
        this.vid = vid;
    }

    public String getPropValuesString() {
        return String.join(",", propValues);
    }

    public List<String> getPropValues() {
        return propValues;
    }

    public void setPropValues(List<String> propValues) {
        this.propValues = propValues;
    }

    @Override
    public String toString() {
        return "NebulaVertex{"
                + "vid='" + vid + '\''
                + ", propValues=" + propValues
                + '}';
    }
}
