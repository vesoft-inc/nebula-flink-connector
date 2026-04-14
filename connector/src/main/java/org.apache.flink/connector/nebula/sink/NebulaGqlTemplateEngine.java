/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.sink;

import java.util.Map;

public final class NebulaGqlTemplateEngine {
    public static final String TABLE_PLACEHOLDER = "{{TABLE}}";

    private NebulaGqlTemplateEngine() {
    }

    public static String render(String template, Map<String, String> values) {
        if (template == null || template.trim().isEmpty()) {
            throw new IllegalArgumentException("gql-template is empty");
        }
        if (!template.contains(TABLE_PLACEHOLDER)) {
            throw new IllegalArgumentException(
                    "gql-template must contain {{TABLE}} placeholder");
        }
        String result = template;
        for (Map.Entry<String, String> entry : values.entrySet()) {
            result = result.replace("{{" + entry.getKey() + "}}", entry.getValue());
        }
        return result;
    }
}

