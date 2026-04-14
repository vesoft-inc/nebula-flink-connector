/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.sink;

import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class NebulaGqlTemplateEngineTest {

    @Test
    public void testRenderTemplate() {
        Map<String, String> values = new HashMap<>();
        values.put("TABLE", "TABLE t{c0} = \n(1)");
        values.put("GRAPH", "g1");
        values.put("TYPE", "person");

        String result = NebulaGqlTemplateEngine.render(
                "{{TABLE}}\nUSE `{{GRAPH}}`\nFOR r IN t\nINSERT (@`{{TYPE}}`{`id`:r.c0})",
                values);

        Assert.assertTrue(result.contains("TABLE t{c0}"));
        Assert.assertTrue(result.contains("USE `g1`"));
        Assert.assertTrue(result.contains("@`person`"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTemplateWithoutTablePlaceholder() {
        NebulaGqlTemplateEngine.render("USE `g1`", new HashMap<>());
    }
}

