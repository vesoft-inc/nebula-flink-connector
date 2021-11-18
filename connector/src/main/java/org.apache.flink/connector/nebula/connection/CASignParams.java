/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.connection;

import java.io.Serializable;

public class CASignParams implements Serializable {

    private String caCrtFilePath;
    private String crtFilePath;
    private String keyFilePath;

    public CASignParams(String caCrtFilePath, String crtFilePath, String keyFilePath) {
        this.caCrtFilePath = caCrtFilePath;
        this.crtFilePath = crtFilePath;
        this.keyFilePath = keyFilePath;
    }

    public String getCaCrtFilePath() {
        return caCrtFilePath;
    }

    public void setCaCrtFilePath(String caCrtFilePath) {
        this.caCrtFilePath = caCrtFilePath;
    }

    public String getCrtFilePath() {
        return crtFilePath;
    }

    public void setCrtFilePath(String crtFilePath) {
        this.crtFilePath = crtFilePath;
    }

    public String getKeyFilePath() {
        return keyFilePath;
    }

    public void setKeyFilePath(String keyFilePath) {
        this.keyFilePath = keyFilePath;
    }

    @Override
    public String toString() {
        return "CASSLSignParams{"
                + "caCrtFilePath='" + caCrtFilePath + '\''
                + ", crtFilePath='" + crtFilePath + '\''
                + ", keyFilePath='" + keyFilePath + '\''
                + '}';
    }
}
