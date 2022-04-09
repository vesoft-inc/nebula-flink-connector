/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.graph.descriptors;

import static org.apache.flink.graph.descriptors.NebulaCatalogValidator.CATALOG_NEBULA_ADDRESS;
import static org.apache.flink.graph.descriptors.NebulaCatalogValidator.CATALOG_NEBULA_PASSWORD;
import static org.apache.flink.graph.descriptors.NebulaCatalogValidator.CATALOG_NEBULA_USERNAME;
import static org.apache.flink.graph.descriptors.NebulaCatalogValidator.CATALOG_PROPERTY_VERSION;
import static org.apache.flink.graph.descriptors.NebulaCatalogValidator.CATALOG_TYPE;
import static org.apache.flink.graph.descriptors.NebulaCatalogValidator.CATALOG_TYPE_VALUE_NEBULA;
import static org.apache.flink.util.Preconditions.checkArgument;

import java.util.Map;
import org.apache.flink.table.descriptors.Descriptor;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.util.StringUtils;

public class NebulaCatalogDescriptor implements Descriptor {
    private final String address;
    private final String username;
    private final String password;

    public NebulaCatalogDescriptor(String address, String username, String password) {
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(address));
        // Nebula 1.1.0 allow no username and password
        //checkArgument(!StringUtils.isNullOrWhitespaceOnly(username));
        //checkArgument(!StringUtils.isNullOrWhitespaceOnly(password));

        this.address = address;
        this.username = username;
        this.password = password;
    }

    @Override
    public Map<String, String> toProperties() {
        final DescriptorProperties properties = new DescriptorProperties();

        properties.putString(CATALOG_TYPE, CATALOG_TYPE_VALUE_NEBULA);
        properties.putLong(CATALOG_PROPERTY_VERSION, 1);

        properties.putString(CATALOG_NEBULA_ADDRESS, address);
        properties.putString(CATALOG_NEBULA_USERNAME, username);
        properties.putString(CATALOG_NEBULA_PASSWORD, password);
        return properties.asMap();
    }
}
