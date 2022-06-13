/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.catalog.factory;

import static org.apache.flink.connector.nebula.table.NebulaDynamicTableFactory.CONNECT_RETRY;
import static org.apache.flink.connector.nebula.table.NebulaDynamicTableFactory.CONNECT_TIMEOUT;
import static org.apache.flink.connector.nebula.table.NebulaDynamicTableFactory.EXECUTE_RETRY;
import static org.apache.flink.connector.nebula.table.NebulaDynamicTableFactory.GRAPHADDRESS;
import static org.apache.flink.connector.nebula.table.NebulaDynamicTableFactory.GRAPH_SPACE;
import static org.apache.flink.connector.nebula.table.NebulaDynamicTableFactory.LABEL_NAME;
import static org.apache.flink.connector.nebula.table.NebulaDynamicTableFactory.METAADDRESS;
import static org.apache.flink.connector.nebula.table.NebulaDynamicTableFactory.PASSWORD;
import static org.apache.flink.connector.nebula.table.NebulaDynamicTableFactory.TIMEOUT;
import static org.apache.flink.connector.nebula.table.NebulaDynamicTableFactory.USERNAME;

import java.util.HashSet;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NebulaCatalogFactory implements CatalogFactory {
    private static final Logger LOG = LoggerFactory.getLogger(NebulaCatalogFactory.class);

    @Override
    public Catalog createCatalog(Context context) {
        return CatalogFactory.super.createCatalog(context);
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(GRAPHADDRESS);
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(METAADDRESS);
        options.add(GRAPH_SPACE);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(LABEL_NAME);
        options.add(CONNECT_TIMEOUT);
        options.add(CONNECT_RETRY);
        options.add(TIMEOUT);
        options.add(EXECUTE_RETRY);
        return options;
    }
}
