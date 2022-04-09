/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.graph.descriptors;

import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.DescriptorValidator;

/**
 * Validator for {@link org.apache.flink.connector.nebula.catalog.NebulaCatalog}
 */
public class NebulaCatalogValidator implements DescriptorValidator {
    public static final String CATALOG_TYPE_VALUE_NEBULA = "nebula";
    public static final String CATALOG_NEBULA_ADDRESS = "address";
    public static final String CATALOG_NEBULA_USERNAME = "username";
    public static final String CATALOG_NEBULA_PASSWORD = "password";

    public static final String CATALOG_TYPE = "type";
    public static final String CATALOG_PROPERTY_VERSION = "property-version";
    public static final String CATALOG_DEFAULT_DATABASE = "default-database";

    @Override
    public void validate(DescriptorProperties properties) {
        properties.validateString(CATALOG_TYPE, false, 1);
        properties.validateInt(CATALOG_PROPERTY_VERSION, true, 0);
        properties.validateString(CATALOG_DEFAULT_DATABASE, true, 1);

        properties.validateValue(CATALOG_TYPE, CATALOG_TYPE_VALUE_NEBULA, false);
        properties.validateString(CATALOG_NEBULA_ADDRESS, false, 1);
        properties.validateString(CATALOG_NEBULA_USERNAME, true, 1);
        properties.validateString(CATALOG_NEBULA_PASSWORD, true, 1);
    }
}
