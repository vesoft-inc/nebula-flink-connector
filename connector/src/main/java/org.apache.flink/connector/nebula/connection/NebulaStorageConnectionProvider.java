/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package org.apache.flink.connector.nebula.connection;


import java.io.Serializable;

public class NebulaStorageConnectionProvider implements Serializable {

    private static final long serialVersionUID = -3822165815516596188L;

    private NebulaClientOptions nebulaClientOptions;

    public NebulaStorageConnectionProvider(NebulaClientOptions nebulaClientOptions) {
        this.nebulaClientOptions = nebulaClientOptions;
    }

    public NebulaStorageConnectionProvider() {
    }


}
