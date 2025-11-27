/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.catalog;

import static org.apache.flink.connector.nebula.TestConstant.graphAddr;
import static org.apache.flink.connector.nebula.TestConstant.passwd;
import static org.apache.flink.connector.nebula.TestConstant.user;

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.connector.nebula.utils.NebulaCatalogUtils;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.Test;

public class NebulaCatalogCreateGraphTest {

    private static final String              CATALOG_NAME  = "NebulaCatalog";
    private static final String              GRAPH_NAME    = "default";
    private static final Map<String, Object> authInfo     = new HashMap<>();

    @Test
    public void testCreateGraphSpace() {
        NebulaCatalog nebulaCatalog = NebulaCatalogUtils.createNebulaCatalog(
                CATALOG_NAME,
                GRAPH_NAME,
                graphAddr,
                user,
                passwd,
                authInfo,
                3000,
                50000,
                null,
                null,
                null,
                null,
                null,
                null,
                false,
                false,
                null,
                null,
                null);

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        tableEnv.registerCatalog(CATALOG_NAME, nebulaCatalog);
        tableEnv.useCatalog(CATALOG_NAME);

        String createDataBase1 = "CREATE DATABASE IF NOT EXISTS `db1`"
                + " COMMENT 'graph 1'"
                + " WITH ("
                + " 'graph_type' = 'flink_catalog_type'"
                + ")";

        String createDataBase2 = "CREATE DATABASE IF NOT EXISTS `db2`"
                + " COMMENT 'graph 2'"
                + " WITH ("
                + " 'graph_type' = 'flink_catalog_type'"
                + ")";

        String createSameDataBase = "CREATE DATABASE IF NOT EXISTS `db1`"
                + " COMMENT 'graph 1'"
                + " WITH ("
                + " 'graph_type' = 'flink_catalog_type'"
                + ")";

        tableEnv.executeSql(createDataBase1);
        tableEnv.executeSql(createDataBase2);
        tableEnv.executeSql(createSameDataBase);
    }
}
