/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.utils;

import java.io.Serializable;
import java.util.Map;

public class NebulaSpace implements Serializable {

    private String spaceName;
    private String comment;
    private Map<String, String> props;

    public NebulaSpace() {
    }

    public NebulaSpace(String spaceName, String comment, Map<String, String> props) {
        this.spaceName = spaceName;
        this.comment = comment;
        this.props = props;
    }

    public String getSpaceName() {
        return spaceName;
    }

    public void setSpaceName(String spaceName) {
        this.spaceName = spaceName;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public Map<String, String> getProps() {
        return props;
    }

    public void setProps(Map<String, String> props) {
        this.props = props;
    }

    @Override
    public String toString() {
        return "NebulaSpace{"
                + "spaceName='" + spaceName + '\''
                + ", comment='" + comment + '\''
                + ", props=" + props
                + '}';
    }
}
