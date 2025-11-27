/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.options;

import static org.apache.flink.connector.nebula.utils.NebulaConstant.DEFAULT_CONNECTION_TIMEOUT_MS;
import static org.apache.flink.connector.nebula.utils.NebulaConstant.DEFAULT_REQUEST_TIMEOUT_MS;

import java.io.Serializable;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

public class ConnectionOptions implements Serializable {
    private final String              graphAddress;
    private final String              user;
    private final Map<String, Object> authInfo;
    private final int                 connectionTimeout;
    private final int                 requestTimeout;
    private final String              schema;
    private final ZoneId              timeZone;
    private final String              dateFormat;
    private final String              localDatetimeFormat;
    private final String              zonedDatetimeFormat;
    private final String              localTimeFormat;
    private final String              zonedTimeFormat;
    private final boolean             enableTls;
    private final boolean             disableVerifyServerCertificate;
    private final String              tlsCaPath;
    private final String              tlsCertPath;
    private final String              tlsKeyPath;

    private ConnectionOptions(String graphAddress,
                              String user,
                              Map<String, Object> authInfo,
                              int connectionTimeout,
                              int requestTimeout,
                              String schema,
                              ZoneId timeZone,
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
        this.graphAddress = graphAddress;
        this.user = user;
        this.authInfo = authInfo;
        this.connectionTimeout = connectionTimeout;
        this.requestTimeout = requestTimeout;
        this.schema = schema;
        this.timeZone = timeZone;
        this.dateFormat = dateFormat;
        this.localDatetimeFormat = localDatetimeFormat;
        this.zonedDatetimeFormat = zonedDatetimeFormat;
        this.localTimeFormat = localTimeFormat;
        this.zonedTimeFormat = zonedTimeFormat;
        this.enableTls = enableTls;
        this.disableVerifyServerCertificate = disableVerifyServerCertificate;
        this.tlsCaPath = tlsCaPath;
        this.tlsCertPath = tlsCertPath;
        this.tlsKeyPath = tlsKeyPath;
    }

    public String getGraphAddress() {
        return graphAddress;
    }

    public String getUser() {
        return user;
    }

    public Map<String, Object> getAuthInfo() {
        return authInfo;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public int getRequestTimeout() {
        return requestTimeout;
    }

    public String getSchema() {
        return schema;
    }

    public ZoneId getTimeZone() {
        return timeZone;
    }

    public String getDateFormat() {
        return dateFormat;
    }

    public String getLocalDatetimeFormat() {
        return localDatetimeFormat;
    }

    public String getZonedDatetimeFormat() {
        return zonedDatetimeFormat;
    }

    public String getLocalTimeFormat() {
        return localTimeFormat;
    }

    public String getZonedTimeFormat() {
        return zonedTimeFormat;
    }

    public boolean isEnableTls() {
        return enableTls;
    }

    public boolean isDisableVerifyServerCertificate() {
        return disableVerifyServerCertificate;
    }

    public String getTlsCaPath() {
        return tlsCaPath;
    }

    public String getTlsCertPath() {
        return tlsCertPath;
    }

    public String getTlsKeyPath() {
        return tlsKeyPath;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String              graphAddress;
        private String              user;
        private String              password;
        private Map<String, Object> authInfo                       = new HashMap<>();
        private int                 connectionTimeout              = DEFAULT_CONNECTION_TIMEOUT_MS;
        private int                 requestTimeout                 = DEFAULT_REQUEST_TIMEOUT_MS;
        private String              schema;
        private ZoneId              timeZone;
        private String              dateFormat;
        private String              localDatetimeFormat;
        private String              zonedDatetimeFormat;
        private String              localTimeFormat;
        private String              zonedTimeFormat;
        private boolean             enableTls                      = false;
        private boolean             disableVerifyServerCertificate = false;
        private String              tlsCaPath;
        private String              tlsCertPath;
        private String              tlsKeyPath;

        public Builder withGraphAddress(String graphAddress) {
            this.graphAddress = graphAddress;
            return this;
        }

        public Builder withUser(String user) {
            this.user = user;
            return this;
        }

        public Builder withPassword(String password) {
            this.password = password;
            return this;
        }

        public Builder withAuthInfo(Map<String, Object> authInfo) {
            this.authInfo = authInfo;
            return this;
        }

        public Builder withConnectionTimeout(int connectionTimeout) {
            if (connectionTimeout <= 0) {
                this.connectionTimeout = Integer.MAX_VALUE;
            } else {
                this.connectionTimeout = connectionTimeout;
            }
            return this;
        }

        public Builder withRequestTimeout(int requestTimeout) {
            if (requestTimeout <= 0) {
                this.requestTimeout = Integer.MAX_VALUE;
            } else {
                this.requestTimeout = requestTimeout;
            }
            return this;
        }

        public Builder withSchema(String schema) {
            if (schema != null && !schema.isEmpty()) {
                this.schema = schema;
            }
            return this;
        }

        public Builder withTimeZone(ZoneId timeZone) {
            if (timeZone != null) {
                this.timeZone = timeZone;
            }
            return this;
        }

        public Builder withDateFormat(String dateFormat) {
            if (dateFormat != null && !dateFormat.isEmpty()) {
                this.dateFormat = dateFormat;
            }
            return this;
        }

        public Builder withLocalDatetimeFormat(String localDatetimeFormat) {
            if (localDatetimeFormat != null && !localDatetimeFormat.isEmpty()) {
                this.localDatetimeFormat = localDatetimeFormat;
            }
            return this;
        }

        public Builder withZonedDatetimeFormat(String zonedDatetimeFormat) {
            if (zonedDatetimeFormat != null && !zonedDatetimeFormat.isEmpty()) {
                this.zonedDatetimeFormat = zonedDatetimeFormat;
            }
            return this;
        }

        public Builder withLocalTimeFormat(String localTimeFormat) {
            if (localTimeFormat != null && !localTimeFormat.isEmpty()) {
                this.localTimeFormat = localTimeFormat;
            }
            return this;
        }

        public Builder withZonedTimeFormat(String zonedTimeFormat) {
            if (zonedTimeFormat != null && !zonedTimeFormat.isEmpty()) {
                this.zonedTimeFormat = zonedTimeFormat;
            }
            return this;
        }

        public Builder withEnableTls(boolean enableTls) {
            this.enableTls = enableTls;
            return this;
        }

        public Builder withDisableVerifyServerCertificate(boolean disableVerifyServerCertificate) {
            this.disableVerifyServerCertificate = disableVerifyServerCertificate;
            return this;
        }

        public Builder withTlsCaPath(String tlsCaPath) {
            this.tlsCaPath = tlsCaPath;
            return this;
        }

        public Builder withTlsCertPath(String tlsCertPath) {
            this.tlsCertPath = tlsCertPath;
            return this;
        }

        public Builder withTlsKeyPath(String tlsKeyPath) {
            this.tlsKeyPath = tlsKeyPath;
            return this;
        }

        public ConnectionOptions build() {
            if (password != null) {
                authInfo.put("password", password);
            }
            if (graphAddress == null || graphAddress.isEmpty()) {
                throw new IllegalArgumentException("graph address is null");
            }
            if (user == null || user.isEmpty()) {
                throw new IllegalArgumentException("user is null");
            }
            return new ConnectionOptions(
                    graphAddress,
                    user,
                    authInfo,
                    connectionTimeout,
                    requestTimeout,
                    schema,
                    timeZone,
                    dateFormat,
                    localDatetimeFormat,
                    zonedDatetimeFormat,
                    localTimeFormat,
                    zonedTimeFormat,
                    enableTls,
                    disableVerifyServerCertificate,
                    tlsCaPath,
                    tlsCertPath,
                    tlsKeyPath
            );
        }
    }
}
