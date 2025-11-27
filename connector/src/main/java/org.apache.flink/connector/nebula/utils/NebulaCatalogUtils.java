/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.utils;

import java.util.Map;
import org.apache.flink.connector.nebula.catalog.NebulaCatalog;
import org.apache.flink.connector.nebula.options.ConnectionOptions;

/**
 * util for {@link NebulaCatalog}
 */
public class NebulaCatalogUtils {

    /**
     * Create catalog instance from given information
     */
    public static NebulaCatalog createNebulaCatalog(
            String catalogName,
            String defaultSpace,
            String graphAddress,
            String username,
            String password,
            Map<String, Object> authInfo,
            int connectionTimeout,
            int requestTimeout,
            String schema,
            String dateFormat,
            String localDatetimeFormat,
            String zonedDatetimeFormat,
            String localTimeFormat,
            String zonedTimeFormat,
            boolean enableTls,
            boolean disableVerifyServerCertificate,
            String tlsCaPath,
            String tlsCertPath,
            String tlsKeyPath) {
        ConnectionOptions connectionOptions = ConnectionOptions
                .builder()
                .withGraphAddress(graphAddress)
                .withUser(username)
                .withPassword(password)
                .withAuthInfo(authInfo)
                .withConnectionTimeout(connectionTimeout)
                .withRequestTimeout(requestTimeout)
                .withSchema(schema)
                .withDateFormat(dateFormat)
                .withLocalDatetimeFormat(localDatetimeFormat)
                .withZonedDatetimeFormat(zonedDatetimeFormat)
                .withLocalTimeFormat(localTimeFormat)
                .withZonedTimeFormat(zonedTimeFormat)
                .withEnableTls(enableTls)
                .withDisableVerifyServerCertificate(disableVerifyServerCertificate)
                .withTlsCaPath(tlsCaPath)
                .withTlsCertPath(tlsCertPath)
                .withTlsKeyPath(tlsKeyPath)
                .build();
        return new NebulaCatalog(catalogName, defaultSpace, connectionOptions);
    }
}
