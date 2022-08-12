/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.utils;

import java.io.Serializable;
import java.util.Map;

public class NebulaSpaces implements Serializable {

    private final NebulaSpace nebulaSpace;
    private final Map<String, String> props;

    public NebulaSpaces(NebulaSpace nebulaSpace) {
        this.nebulaSpace = nebulaSpace;
        this.props = nebulaSpace.getProps();
    }

    /**
     * construct Nebula create space ngql.
     *
     * @return ngql
     */
    public String getCreateStatement() {
        Map<String, String> props = nebulaSpace.getProps();
        StringBuilder sb = new StringBuilder();
        addParams(sb, NebulaConstant.CREATE_PARTITION_NUM, NebulaConstant.DEFAULT_PARTITION_NUM);
        addParams(sb, NebulaConstant.CREATE_REPLICA_FACTOR, NebulaConstant.DEFAULT_REPLICA_FACTOR);
        sb.append(NebulaConstant.CREATE_VID_TYPE)
                .append(" = ")
                .append(props.get(NebulaConstant.CREATE_VID_TYPE));

        String stat = String.format(
                NebulaConstant.CREATE_SPACE_TEMPLATE,
                nebulaSpace.getSpaceName(),
                sb
        );

        String comment = nebulaSpace.getComment();
        if (comment != null) {
            stat += String.format(NebulaConstant.CREATE_SPACE_COMMENT, comment);
        }

        return stat;
    }

    private void addParams(StringBuilder sb, String para, int defaultValue) {
        sb.append(para)
            .append(" = ")
            .append(props.containsKey(para)
                  ? Integer.parseInt(props.get(para))
                  : defaultValue)
            .append(", ");
    }
}
