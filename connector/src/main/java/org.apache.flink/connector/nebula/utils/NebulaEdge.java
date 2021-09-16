/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package org.apache.flink.connector.nebula.utils;

import java.io.Serializable;
import java.util.List;

public class NebulaEdge implements Serializable {
    private String source;
    private String target;
    private Long rank;
    private List<String> propValues;

    public NebulaEdge(String source, String target, Long rank, List<String> propValues) {
        this.source = source;
        this.target = target;
        this.rank = rank;
        this.propValues = propValues;
    }

    public String getSource() {
        return source;
    }

    public String getTarget() {
        return target;
    }

    public Long getRank() {
        return rank;
    }

    public List<String> getPropValues() {
        return propValues;
    }

    public String getPropValuesString() {
        return String.join(",", propValues);
    }

    @Override
    public String toString() {
        return "NebulaEdge{"
                + "source='" + source + '\''
                + ", target='" + target + '\''
                + ", rank=" + rank
                + ", propValues=" + propValues
                + '}';
    }
}
