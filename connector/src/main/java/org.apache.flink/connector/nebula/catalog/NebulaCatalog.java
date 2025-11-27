/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.catalog;

import static org.apache.flink.connector.nebula.table.NebulaDynamicTableFactory.DATA_TYPE;
import static org.apache.flink.connector.nebula.table.NebulaDynamicTableFactory.EDGE_PATTERN_TYPE;
import static org.apache.flink.connector.nebula.table.NebulaDynamicTableFactory.GRAPH_NAME;
import static org.apache.flink.connector.nebula.table.NebulaDynamicTableFactory.IDENTIFIER;
import static org.apache.flink.connector.nebula.table.NebulaDynamicTableFactory.LABEL_NAME;
import static org.apache.flink.connector.nebula.table.NebulaDynamicTableFactory.PASSWORD;
import static org.apache.flink.connector.nebula.table.NebulaDynamicTableFactory.USERNAME;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly;

import com.vesoft.nebula.driver.graph.data.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.collections.map.HashedMap;
import org.apache.flink.connector.nebula.options.ConnectionOptions;
import org.apache.flink.connector.nebula.utils.NebulaGraph;
import org.apache.flink.connector.nebula.utils.NebulaUtils;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class NebulaCatalog extends AbstractNebulaCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(NebulaCatalog.class);

    // graphName -> NODE -> [node types]
    // graphName -> EDGE -> [edge types]
    private Map<String, Map<String, HashSet<String>>> graphDataTypes          = new HashMap<>();
    private Map<String, String>                       graphName2GraphTypeName = new HashMap<>();

    public NebulaCatalog(
            String catalogName,
            @Nullable String defaultDatabase,
            ConnectionOptions connectionOptions) {
        super(catalogName, defaultDatabase, connectionOptions);
    }

    @Override
    public void open() throws CatalogException {
        super.open();
    }

    @Override
    public void close() throws CatalogException {
        super.close();
        if (graphProvider != null) {
            graphProvider.close();
        }
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        List<String> graphNames = new ArrayList<>();
        try {
            ResultSet resultSet = graphProvider.execute("SHOW GRAPHS");
            if (!resultSet.isSucceeded()) {
                LOG.error("listDatabases with `SHOW GRAPHS` failed:" + resultSet.getErrorMessage());
                throw new CatalogException("listDatabases failed:" + resultSet.getErrorMessage());
            }
            while (resultSet.hasNext()) {
                graphNames.add(resultSet.next().get("name").asString());
            }
        } catch (Exception e) {
            LOG.error("listDatabases with `SHOW GRAPHS` error", e);
            throw new CatalogException(e);
        }
        return graphNames;
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException,
                                                                   CatalogException {
        if (listDatabases().contains(databaseName.trim())) {
            Map<String, String> props = new HashedMap();
            try {
                ResultSet showGraphResult = graphProvider.execute(
                        "DESC GRAPH `" + NebulaUtils.escape(databaseName) + "`");
                props.put("name", databaseName);
                while (showGraphResult.hasNext()) {
                    ResultSet.Record record = showGraphResult.next();
                    if (record.get("name").asString().equals(databaseName)) {
                        props.put("graph_type", record.get("graph_type").toString());
                        props.put("schema", record.get("schema").toString());
                        props.put("owner", record.get("owner").toString());
                    }
                }
            } catch (Exception e) {
                LOG.error("get graph " + databaseName + " error, ", e);
                throw new CatalogException("getDatabase error: " + e.getMessage(), e);
            }
            return new CatalogDatabaseImpl(props, databaseName);
        } else {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
    }

    /**
     * @param dataBaseName    same as graph space name in nebula graph
     * @param catalogDatabase catalog implementation
     * @param ignoreIfExists  true if contains [if not exists] clause else false
     */
    @Override
    public void createDatabase(String dataBaseName,
                               CatalogDatabase catalogDatabase,
                               boolean ignoreIfExists)
            throws CatalogException {
        checkArgument(
                !isNullOrWhitespaceOnly(dataBaseName), "graph name cannot be null or empty.");
        checkNotNull(catalogDatabase, "graph name cannot be null.");

        if (ignoreIfExists && listDatabases().contains(dataBaseName)) {
            LOG.info("Repeat to create graph, {} already exists, no effect.", dataBaseName);
            return;
        }
        Map<String, String> properties = catalogDatabase.getProperties();

        NebulaGraph nebulaGraph = new NebulaGraph(dataBaseName, properties);

        ResultSet execResult = null;
        try {
            execResult = graphProvider.execute(nebulaGraph.getCreateGraphType(ignoreIfExists));
        } catch (Exception e) {
            LOG.error("nebula create graph type failed.", e);
            throw new CatalogException("nebula create graph type failed.", e);
        }

        if (execResult.isSucceeded()) {
            LOG.debug("create graph type success.");
        } else {
            LOG.error("create graph type failed: {}", execResult.getErrorMessage());
            throw new CatalogException("create graph type failed, " + execResult.getErrorMessage());
        }

        try {
            execResult = graphProvider.execute(nebulaGraph.getCreateGraph(ignoreIfExists));
        } catch (Exception e) {
            LOG.error("nebula create graph failed.", e);
            throw new CatalogException("nebula create graph failed.", e);
        }

        if (execResult.isSucceeded()) {
            LOG.debug("create graph success.");
        } else {
            LOG.error("create graph failed: {}", execResult.getErrorMessage());
            throw new CatalogException("create graph failed, " + execResult.getErrorMessage());
        }
    }

    /**
     * check if NodeType or EdgeType exists in graph
     *
     * @param tablePath A graph name and label name.
     * @return Table exists or not
     */
    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        String graphName = tablePath.getDatabaseName();
        String table     = tablePath.getObjectName();
        try {
            if (graphDataTypes.containsKey(graphName)) {
                Map<String, HashSet<String>> typeNamesMap = graphDataTypes.get(graphName);
                for (Map.Entry<String, HashSet<String>> types : typeNamesMap.entrySet()) {
                    if (types.getValue().contains(table)) {
                        return true;
                    }
                }
                return false;
            }
            return (listTables(graphName).contains(table));
        } catch (DatabaseNotExistException e) {
            throw new CatalogException("failed to call tableExists function, ", e);
        }
    }


    /**
     * show all node types and edge types
     *
     * @param graphName nebula graph name
     * @return List of NodeType and EdgeType.
     */
    @Override
    public List<String> listTables(String graphName) throws DatabaseNotExistException,
                                                            CatalogException {
        if (!databaseExists(graphName)) {
            throw new DatabaseNotExistException(getName(), graphName);
        }
        if (!graphDataTypes.containsKey(graphName)) {
            graphDataTypes.put(graphName, new HashMap<>());
        }

        String       graphType = null;
        List<String> tables    = new ArrayList<>();
        try {
            graphType = getGraphType(graphName);
            graphName2GraphTypeName.put(graphName, graphType);
            ResultSet resultSet = graphProvider.execute("DESC GRAPH TYPE " + graphType);
            if (!resultSet.isSucceeded()) {
                LOG.error(String.format(
                        "listTables with `DESC GRAPH TYPE %s` failed: %s",
                        graphType, resultSet.getErrorMessage()));
                throw new CatalogException(String.format(
                        "listTables with `DESC GRAPH TYPE %s` failed: %s",
                        graphType,
                        resultSet.getErrorMessage()));
            }
            while (resultSet.hasNext()) {
                ResultSet.Record record     = resultSet.next();
                String           typeName   = record.get("type_name").asString();
                String           entityType = record.get("entity_type").asString();
                if (!graphDataTypes.get(graphName).containsKey(entityType)) {
                    graphDataTypes.get(graphName).put(entityType, new HashSet<>());
                }
                graphDataTypes.get(graphName).get(entityType).add(typeName);
                tables.add(typeName);
            }
        } catch (Exception e) {
            LOG.error(String.format("listTables with `DESC GRAPH TYPE %s` error", graphType), e);
            throw new CatalogException(
                    String.format("listTables with `DESC GRAPH TYPE %s` error", graphType), e);
        }
        return tables;
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException,
                                                                  CatalogException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(getName(), tablePath);
        }

        String graphName = tablePath.getDatabaseName();
        String typeName  = tablePath.getObjectName();
        String dataType  = null;
        String graphType = getGraphType(graphName);

        for (Map.Entry<String, HashSet<String>> types : graphDataTypes.get(graphName).entrySet()) {
            if (types.getValue().contains(typeName)) {
                dataType = types.getKey();
            }
        }
        if (dataType == null) {
            throw new TableNotExistException(getName(), tablePath);
        }

        Map<String, String> props = new HashMap<>();
        props.put(CONNECTOR.key(), IDENTIFIER);
        props.put(USERNAME.key(), connectionOptions.getUser());
        props.put(PASSWORD.key(), (String) connectionOptions.getAuthInfo().get("password"));
        props.put(GRAPH_NAME.key(), tablePath.getDatabaseName());
        props.put(LABEL_NAME.key(), tablePath.getObjectName());
        props.put(DATA_TYPE.key(), dataType);

        TableSchema tableSchema = null;
        ResultSet   descTypeRes = null;
        try {
            if ("Node".equalsIgnoreCase(dataType)) {
                descTypeRes = graphProvider.execute(String.format("DESC NODE TYPE `%s` OF `%s`",
                                                                  typeName,
                                                                  graphType));
            } else {
                descTypeRes = graphProvider.execute(String.format("DESC EDGE TYPE `%s` OF `%s`",
                                                                  typeName,
                                                                  graphType));
                ResultSet edgePatternRes = graphProvider.execute(String.format(
                        "CALL describe_graph_type(\"%s\")filter type_name='%s' return type_pattern",
                        graphType,
                        typeName));
                String pattern = null;
                if (edgePatternRes.hasNext()) {
                    pattern = edgePatternRes.next().get("type_pattern").asString();
                }
                props.put(EDGE_PATTERN_TYPE.key(), pattern);
            }
        } catch (Exception e) {
            throw new CatalogException(e);
        }

        int size = (int) descTypeRes.rowSize();
        if (size == 0) {
            tableSchema = new TableSchema.Builder().build();
        } else {
            String[]   names = new String[size];
            DataType[] types = new DataType[size];
            int        index = 0;
            while (descTypeRes.hasNext()) {
                ResultSet.Record record = descTypeRes.next();
                names[index] = record.get("property_name").asString();
                types[index] = fromNebulaType(record.get("data_type").asString());
                index++;
            }
            tableSchema = new TableSchema.Builder().fields(names, types).build();
        }


        return new CatalogTableImpl(tableSchema, props, "nebulaTableCatalog");
    }


    /**
     * construct flink datatype from nebula type
     */
    private DataType fromNebulaType(String nebulaDataType) {

        switch (nebulaDataType) {
            case "INT8":
            case "UINT8":
            case "INT16":
            case "UINT16":
            case "INT32":
            case "UINT32":
                return DataTypes.INT();
            case "INT64":
            case "UINT64":
                return DataTypes.BIGINT();
            case "BOOL":
                return DataTypes.BOOLEAN();
            case "FLOAT":
                return DataTypes.FLOAT();
            case "DOUBLE":
                return DataTypes.DOUBLE();

            case "DATE":
                return DataTypes.DATE();
            case "LOCAL DATETIME":
            case "LOCAL TIME":
                return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE();
            case "ZONED DATETIME":
            case "ZONED TIME":
                return DataTypes.TIMESTAMP_WITH_TIME_ZONE();
            case "STRING":
                return DataTypes.STRING();
            default:
                throw new UnsupportedOperationException(String.format("Doesn't support nebula "
                                                                              + "type '%s' yet",
                                                                      nebulaDataType));
        }
    }


    private String getGraphType(String graphName) {
        if (!graphName2GraphTypeName.containsKey(graphName)) {
            try {
                String graphType = graphProvider.getGraphType(graphName);
                graphName2GraphTypeName.put(graphName, graphType);
            } catch (Exception e) {
                LOG.error("get graph type error", e);
                throw new CatalogException("get graph type error", e);
            }
        }
        return graphName2GraphTypeName.get(graphName);
    }
}
