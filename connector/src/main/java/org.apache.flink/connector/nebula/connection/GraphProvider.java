/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.connection;

import com.vesoft.nebula.driver.graph.data.ResultSet;
import com.vesoft.nebula.driver.graph.data.ValueWrapper;
import com.vesoft.nebula.driver.graph.net.NebulaClient;
import com.vesoft.nebula.driver.graph.net.NebulaPool;
import com.vesoft.nebula.driver.graph.scan.ScanEdgeResultIterator;
import com.vesoft.nebula.driver.graph.scan.ScanNodeResultIterator;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.flink.connector.nebula.options.ConnectionOptions;
import org.apache.flink.connector.nebula.utils.NebulaEdgeSchema;
import org.apache.flink.connector.nebula.utils.NebulaNodeSchema;
import org.apache.flink.connector.nebula.utils.NebulaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphProvider implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(GraphProvider.class);

    private static final long serialVersionUID = 8392002706492085208L;

    private final ConnectionOptions connectionOptions;
    private final NebulaPool        pool;

    public GraphProvider(ConnectionOptions connectionOptions) {
        this.connectionOptions = connectionOptions;
        try {
            pool = getNebulaPool();
        } catch (Exception e) {
            throw new RuntimeException("create Nebula Client failed:" + e.getMessage(), e);
        }
    }

    /**
     * get Session to execute query statement
     */
    private NebulaPool getNebulaPool() throws Exception {

        List<String> addresses = Arrays.asList(connectionOptions.getGraphAddress().split(","));
        Collections.shuffle(addresses);
        NebulaPool.Builder builder = NebulaPool
                .builder(String.join(",", addresses), connectionOptions.getUser())
                .withAuthOptions(connectionOptions.getAuthInfo())
                .withConnectTimeoutMills(connectionOptions.getConnectionTimeout())
                .withRequestTimeoutMills(connectionOptions.getRequestTimeout())
                .withBlockWhenExhausted(true)
                .withMaxWaitMills(Long.MAX_VALUE)
                .withLifo(false);

        if (connectionOptions.getSchema() != null) {
            builder.withSchema(connectionOptions.getSchema());
        }
        if (connectionOptions.getLocalDatetimeFormat() != null) {
            builder.withLocalDatetimeFormat(connectionOptions.getLocalDatetimeFormat());
        }
        if (connectionOptions.getLocalTimeFormat() != null) {
            builder.withLocalTimeFormat(connectionOptions.getLocalTimeFormat());
        }
        if (connectionOptions.getZonedDatetimeFormat() != null) {
            builder.withZonedDatetimeFormat(connectionOptions.getZonedDatetimeFormat());
        }
        if (connectionOptions.getZonedTimeFormat() != null) {
            builder.withZonedTimeFormat(connectionOptions.getZonedTimeFormat());
        }
        return builder.build();
    }


    public ResultSet execute(String statement) throws Exception {
        NebulaClient client = null;
        ResultSet    res    = null;
        try {
            client = pool.getClient();
            res = client.execute(statement);
        } finally {
            if (client != null) {
                pool.returnClient(client);
            }
        }
        return res;
    }

    public ScanNodeResultIterator scanNode(String schema,
                                           String graphName,
                                           String nodeType,
                                           List<String> returnColumns,
                                           int partId,
                                           int limit) {
        NebulaClient           client   = null;
        ScanNodeResultIterator iterator = null;
        try {
            client = pool.getClient();
            iterator = client.scanNode(schema, graphName, nodeType, returnColumns, partId, limit);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (client != null) {
                pool.returnClient(client);
            }
        }
        return iterator;
    }


    public ScanEdgeResultIterator scanEdge(String schema,
                                           String graphName,
                                           String edgeType,
                                           List<String> returnColumns,
                                           int partId,
                                           int limit) {
        NebulaClient           client   = null;
        ScanEdgeResultIterator iterator = null;
        try {
            client = pool.getClient();
            iterator = client.scanEdge(schema, graphName, edgeType, returnColumns, partId, limit);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (client != null) {
                pool.returnClient(client);
            }
        }
        return iterator;
    }


    /**
     * get node schema
     *
     * @param graphName graph name
     * @param nodeType  node type name
     */
    public NebulaNodeSchema getNodeSchema(String graphName, String nodeType)
            throws Exception {
        NebulaNodeSchema nodeSchema = new NebulaNodeSchema();
        String           graphType  = getGraphType(NebulaUtils.escape(graphName));

        ResultSet result = execute(
                String.format("DESCRIBE NODE TYPE `%s` OF `%s`",
                              NebulaUtils.escape(nodeType),
                              NebulaUtils.escape(graphType)));
        if (!result.isSucceeded() || result.isEmpty()) {
            throw new IllegalArgumentException(
                    "node type " + nodeType + " does not exist in " + graphName);
        }

        List<String>        pks        = new ArrayList<>();
        List<String>        propNames  = new ArrayList<>();
        Map<String, String> properties = new HashMap<>();
        while (result.hasNext()) {
            ResultSet.Record record   = result.next();
            String           propName = record.get("property_name").asString();
            propNames.add(propName);
            properties.put(propName, record.get("data_type").asString());
            if ("Y".equals(record.get("primary_key").asString())) {
                pks.add(record.get("property_name").asString());
            }
        }
        nodeSchema.setNodeTypeName(nodeType);
        nodeSchema.setPkNames(pks);
        nodeSchema.setPropNames(propNames);
        nodeSchema.setProperties(properties);
        return nodeSchema;
    }

    /**
     * get edge type schema
     *
     * @param graphName graph name
     * @param edgeType  edge type name
     */
    public NebulaEdgeSchema getEdgeSchema(String graphName, String edgeType)
            throws Exception {

        String graphType = getGraphType(NebulaUtils.escape(graphName));

        String descEdgeType = String.format(
                "CALL describe_graph_type('%s') yield type_name,type_pattern,"
                        + "`primary_key/multiedge_key` as pkk filter type_name='%s' return "
                        + "type_pattern, pkk "
                        + "next OPTIONAL CALL describe_edge_type('%s','%s') return *",
                NebulaUtils.escape(graphType),
                NebulaUtils.escape(edgeType),
                NebulaUtils.escape(graphType),
                NebulaUtils.escape(edgeType));

        ResultSet result = execute(descEdgeType);
        if (!result.isSucceeded() || result.isEmpty()) {
            throw new IllegalArgumentException(
                    "edge type " + edgeType + " does not exist in " + graphName);
        }

        String              edgeTypePattern = null;
        List<String>        multipleEdgeKeys = new ArrayList<>();
        List<String>        propNames       = new ArrayList<>();
        Map<String, String> properties      = new HashMap<>();
        while (result.hasNext()) {
            ResultSet.Record record = result.next();
            if (edgeTypePattern == null) {
                edgeTypePattern = record.get("type_pattern").asString();
                ValueWrapper edgeMultiKeysValue = record.get("pkk");
                if (edgeMultiKeysValue != null && edgeMultiKeysValue.isList()) {
                    for (ValueWrapper col : edgeMultiKeysValue.asList()) {
                        multipleEdgeKeys.add(col.asString());
                    }
                }
            }
            ValueWrapper propName = record.get("property_name");
            if (!propName.isNull()) {
                propNames.add(propName.asString());
                properties.put(propName.asString(), record.get("data_type").asString());
            }
        }

        // get the src node type and dst node type according to edge pattern
        String  srcNodeType                 = null;
        String  dstNodeType                 = null;
        String  edgeDirectionPattern        = "\\((.*?)\\)-\\[.*?\\]->\\((.*?)\\)";
        String  edgeUnDirectionPattern      = "\\((.*?)\\)~\\[.*?\\]~\\((.*?)\\)";
        Pattern patternWithEdgeDirection    = Pattern.compile(edgeDirectionPattern);
        Pattern patternWithoutEdgeDirection = Pattern.compile(edgeUnDirectionPattern);
        Matcher matcherWithEdgeDirection    = patternWithEdgeDirection.matcher(edgeTypePattern);
        Matcher matcherWithoutEdgeDirection = patternWithoutEdgeDirection.matcher(edgeTypePattern);
        if (matcherWithEdgeDirection.matches()) {
            srcNodeType = matcherWithEdgeDirection.group(1);
            dstNodeType = matcherWithEdgeDirection.group(2);
        } else if (matcherWithoutEdgeDirection.matches()) {
            srcNodeType = matcherWithoutEdgeDirection.group(1);
            dstNodeType = matcherWithoutEdgeDirection.group(2);
        } else {
            throw new RuntimeException("Cannot parse the edge type pattern.");
        }

        NebulaEdgeSchema edgeSchema = new NebulaEdgeSchema();
        edgeSchema.setEdgeTypeName(edgeType);
        edgeSchema.setSrcNodeTypeName(srcNodeType);
        edgeSchema.setDstNodeTypeName(dstNodeType);
        NebulaNodeSchema srcNodeSchema = getNodeSchema(graphName, srcNodeType);
        edgeSchema.setSrcPkNames(srcNodeSchema.getPkNames());
        Map<String, String> srcPkDataType = new HashMap<>();
        for (String pk : srcNodeSchema.getPkNames()) {
            srcPkDataType.put(pk, srcNodeSchema.getProperties().get(pk));
        }
        edgeSchema.setSrcPkDataTypeMap(srcPkDataType);
        NebulaNodeSchema dstNodeSchema = getNodeSchema(graphName, dstNodeType);
        edgeSchema.setDstPkNames(dstNodeSchema.getPkNames());
        Map<String, String> dstPkDataType = new HashMap<>();
        for (String pk : dstNodeSchema.getPkNames()) {
            dstPkDataType.put(pk, dstNodeSchema.getProperties().get(pk));
        }
        edgeSchema.setDstPkDataTypeMap(dstPkDataType);
        edgeSchema.setMultipleEdgeKeys(multipleEdgeKeys);
        edgeSchema.setPropNames(propNames);
        edgeSchema.setProperties(properties);
        return edgeSchema;
    }


    /**
     * get the graph type of graph
     *
     * @param graphName graph name
     * @return graph type name
     */
    public String getGraphType(String graphName) throws Exception {
        ResultSet resultSet = execute("DESCRIBE GRAPH `" + graphName + "`");
        String    graphType;
        if (resultSet.isSucceeded() && !resultSet.isEmpty()) {
            graphType = resultSet.next().values().get(1).asString();
        } else {
            throw new IllegalArgumentException("graphName " + graphName + " does not exist.");
        }
        return graphType;
    }

    /**
     * get all part list for NebulaGraph
     */
    public List<Integer> getAllParts() throws Exception {
        String        showPartitions = "CALL show_partitions() RETURN *";
        ResultSet     resultSet      = execute(showPartitions);
        List<Integer> parts          = new ArrayList<>();

        if (resultSet.isSucceeded() && !resultSet.isEmpty()) {
            while (resultSet.hasNext()) {
                int partId = resultSet.next().get("partition_id").asInt();
                if (partId != 0) {
                    parts.add(partId);
                }
            }
            return parts;
        }
        LOG.error("get all partitions failed for {}", resultSet.getErrorMessage());
        throw new RuntimeException("get all partitions failed for " + resultSet.getErrorMessage());
    }


    public void close() {
        pool.close();
    }
}
