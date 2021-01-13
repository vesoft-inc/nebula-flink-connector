/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package org.apache.flink.connector.nebula.source;


import com.vesoft.nebula.client.storage.data.BaseTableRow;

/**
 * converter to convert Nebula Data to [T]
 */
public interface NebulaConverter<T> {

    public T convert(BaseTableRow record);
}
