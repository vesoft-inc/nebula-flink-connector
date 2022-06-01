/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.catalog.factory;

import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.connector.nebula.catalog.NebulaCatalog;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.FactoryUtil.CatalogFactoryHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NebulaCatalogFactory implements CatalogFactory {
    private static final Logger LOG = LoggerFactory.getLogger(NebulaCatalogFactory.class);
    public static final String IDENTIFIER = "nebula";

    public static final ConfigOption<String> DEFAULT_DATABASE = ConfigOptions
            .key("default-database")
            .stringType()
            .defaultValue("default");

    public static final ConfigOption<String> ADDRESS = ConfigOptions
            .key("address")
            .stringType()
            .noDefaultValue()
            .withDescription("the nebula meta server address.");

    public static final ConfigOption<String> USERNAME = ConfigOptions
            .key("username")
            .stringType()
            .noDefaultValue()
            .withDescription("the nebula server name.");

    public static final ConfigOption<String> PASSWORD = ConfigOptions
            .key("password")
            .stringType()
            .noDefaultValue()
            .withDescription("the nebula server password.");

    @Override
    public Catalog createCatalog(Context context) {
        String catalogName = context.getName();
        CatalogFactoryHelper helper = FactoryUtil.createCatalogFactoryHelper(this, context);
        helper.validate();
        String defaultDatabase = helper.getOptions().getOptional(DEFAULT_DATABASE).get();
        String address = helper.getOptions().getOptional(ADDRESS).get();
        String username = helper.getOptions().getOptional(USERNAME).get();
        String password = helper.getOptions().getOptional(PASSWORD).get();

        try {
            return new NebulaCatalog(catalogName,defaultDatabase, username, password, address);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> set = new HashSet<>();
        set.add(ADDRESS);
        set.add(USERNAME);
        set.add(PASSWORD);
        return set;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> set = new HashSet<>();
        set.add(DEFAULT_DATABASE);
        return set;
    }

}
