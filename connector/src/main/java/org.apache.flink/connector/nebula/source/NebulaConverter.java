/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.source;


import com.vesoft.nebula.client.storage.data.BaseTableRow;
import java.io.UnsupportedEncodingException;

/**
 * converter to convert Nebula Data to [T]
 */
public interface NebulaConverter<T> {

    public T convert(BaseTableRow record) throws UnsupportedEncodingException;
}
