/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.utils;

import com.google.common.collect.Lists;
import java.util.List;

/**
 * @author Pan BinBin
 */
public class PartitionUtils {
    public static List<Integer> getScanParts(Integer index,
                                             Integer nebulaTotalPart, Integer numSplit) {
        List<Integer> scanParts = Lists.newArrayList();
        Integer currentPart = index;
        while (currentPart <= nebulaTotalPart) {
            scanParts.add(currentPart);
            currentPart += numSplit;
        }
        return scanParts;
    }
}
