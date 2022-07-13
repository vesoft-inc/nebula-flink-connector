/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.catalog;

import org.apache.flink.connector.nebula.utils.NebulaCatalogUtils;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.Test;

public class NebulaCatalogCreateSpaceTest {

    private static final String CATALOG_NAME = "NebulaCatalog";
    private static final String GRAPH_SPACE = "default";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "nebula";
    private static final String META_ADDRESS = "192.168.200.135:9559";
    private static final String GRAPH_ADDRESS = "192.168.200.135:9669";

    @Test
    public void testCreateGraphSpace() {
        NebulaCatalog nebulaCatalog = NebulaCatalogUtils.createNebulaCatalog(
                CATALOG_NAME,
                GRAPH_SPACE,
                USERNAME,
                PASSWORD,
                META_ADDRESS,
                GRAPH_ADDRESS
        );

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        tableEnv.registerCatalog(CATALOG_NAME, nebulaCatalog);
        tableEnv.useCatalog(CATALOG_NAME);

        String createDataBase1 = "CREATE DATABASE IF NOT EXISTS `db1`"
                + " COMMENT 'space 1'"
                + " WITH ("
                + " 'partition_num' = '100',"
                + " 'replica_factor' = '3',"
                + " 'vid_type' = 'FIXED_STRING(10)'"
                + ")";

        String createDataBase2 = "CREATE DATABASE IF NOT EXISTS `db2`"
                + " COMMENT 'space 2'"
                + " WITH ("
                + " 'partition_num' = '10',"
                + " 'replica_factor' = '2',"
                + " 'vid_type' = 'INT'"
                + ")";

        tableEnv.executeSql(createDataBase1);
        tableEnv.executeSql(createDataBase2);
    }
}
