/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.catalog.factory;

import static org.apache.flink.graph.descriptors.NebulaCatalogValidator.CATALOG_NEBULA_ADDRESS;
import static org.apache.flink.graph.descriptors.NebulaCatalogValidator.CATALOG_NEBULA_PASSWORD;
import static org.apache.flink.graph.descriptors.NebulaCatalogValidator.CATALOG_NEBULA_USERNAME;
import static org.apache.flink.graph.descriptors.NebulaCatalogValidator.CATALOG_TYPE_VALUE_NEBULA;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_DEFAULT_DATABASE;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_TYPE;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.map.HashedMap;
import org.apache.flink.connector.nebula.catalog.NebulaCatalog;
import org.apache.flink.graph.descriptors.NebulaCatalogValidator;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.CatalogFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NebulaCatalogFactory implements CatalogFactory {
    private static final Logger LOG = LoggerFactory.getLogger(NebulaCatalogFactory.class);

    @Override
    public Catalog createCatalog(String name, Map<String, String> properties) {
        final DescriptorProperties prop = getValidatedProperties(properties);
        try {
            return new NebulaCatalog(
                    name,
                    prop.getString(CATALOG_DEFAULT_DATABASE),
                    prop.getString(CATALOG_NEBULA_USERNAME),
                    prop.getString(CATALOG_NEBULA_PASSWORD),
                    prop.getString(CATALOG_NEBULA_ADDRESS));
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException("address is illegal,", e);
        }
    }

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> context = new HashedMap();
        context.put(CATALOG_TYPE, CATALOG_TYPE_VALUE_NEBULA);
        context.put(CATALOG_PROPERTY_VERSION, "1");
        return context;
    }

    @Override
    public List<String> supportedProperties() {
        List<String> properties = new ArrayList<>();

        // default database
        properties.add(CATALOG_DEFAULT_DATABASE);

        properties.add(CATALOG_NEBULA_ADDRESS);
        properties.add(CATALOG_NEBULA_USERNAME);
        properties.add(CATALOG_NEBULA_PASSWORD);
        return properties;
    }

    private static DescriptorProperties getValidatedProperties(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);

        new NebulaCatalogValidator().validate(descriptorProperties);

        return descriptorProperties;
    }
}
