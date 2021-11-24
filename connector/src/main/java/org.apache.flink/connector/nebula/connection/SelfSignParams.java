/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.connection;

import java.io.Serializable;

public class SelfSignParams implements Serializable {

    private String crtFilePath;
    private String keyFilePath;
    private String password;

    public SelfSignParams(String crtFilePath, String keyFilePath, String password) {
        this.crtFilePath = crtFilePath;
        this.keyFilePath = keyFilePath;
        this.password = password;
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

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    @Override
    public String toString() {
        return "SelfSSLSignParams{"
                + "crtFilePath='" + crtFilePath + '\''
                + ", keyFilePath='" + keyFilePath + '\''
                + ", password='" + password + '\''
                + '}';
    }
}
