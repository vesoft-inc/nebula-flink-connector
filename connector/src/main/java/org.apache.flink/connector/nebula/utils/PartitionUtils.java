package org.apache.flink.connector.nebula.utils;

import com.google.common.collect.Lists;
import java.util.List;

/**
 * @author Pan BinBin
 * @date 2022/3/25
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
