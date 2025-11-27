/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.source;


import com.vesoft.nebula.driver.graph.scan.TableRow;
import java.io.UnsupportedEncodingException;

/**
 * converter to convert Nebula Data to [T]
 */
public interface NebulaConverter<T> {

    public T convert(TableRow record) throws UnsupportedEncodingException;
}
