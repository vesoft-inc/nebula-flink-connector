/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.catalog;

import static org.apache.flink.connector.nebula.table.NebulaDynamicTableFactory.GRAPH_SPACE;
import static org.apache.flink.connector.nebula.table.NebulaDynamicTableFactory.IDENTIFIER;
import static org.apache.flink.connector.nebula.table.NebulaDynamicTableFactory.LABEL_NAME;
import static org.apache.flink.connector.nebula.table.NebulaDynamicTableFactory.METAADDRESS;
import static org.apache.flink.connector.nebula.table.NebulaDynamicTableFactory.PASSWORD;
import static org.apache.flink.connector.nebula.table.NebulaDynamicTableFactory.USERNAME;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;

import com.facebook.thrift.TException;
import com.vesoft.nebula.PropertyType;
import com.vesoft.nebula.client.graph.data.HostAddress;
import com.vesoft.nebula.client.graph.exception.ClientServerIncompatibleException;
import com.vesoft.nebula.client.meta.MetaClient;
import com.vesoft.nebula.client.meta.exception.ExecuteFailedException;
import com.vesoft.nebula.meta.ColumnDef;
import com.vesoft.nebula.meta.EdgeItem;
import com.vesoft.nebula.meta.IdName;
import com.vesoft.nebula.meta.Schema;
import com.vesoft.nebula.meta.TagItem;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.map.HashedMap;
import org.apache.flink.connector.nebula.utils.DataTypeEnum;
import org.apache.flink.connector.nebula.utils.NebulaConstant;
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
    private final List<HostAddress> hostAndPorts;
    private final MetaClient metaClient;

    public NebulaCatalog(String catalogName, String defaultDatabase, String username, String pwd,
                         String address) {
        super(catalogName, defaultDatabase, username, pwd, address);
        this.hostAndPorts = NebulaUtils.getHostAndPorts(address);
        this.metaClient = new MetaClient(hostAndPorts);
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        List<String> spaceNames = new ArrayList<>();
        try {
            metaClient.connect();
            List<IdName> spaces = metaClient.getSpaces();
            for (IdName space : spaces) {
                spaceNames.add(new String(space.getName()));
            }
        } catch (TException | ExecuteFailedException | ClientServerIncompatibleException e) {
            LOG.error("failed to connect meta service vis {} ", address, e);
            throw new CatalogException("nebula meta service connect failed.", e);
        }
        return spaceNames;
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException,
            CatalogException {
        if (listDatabases().contains(databaseName.trim())) {
            Map<String, String> props = new HashedMap();
            try {
                props.put("spaceId",
                        String.valueOf(metaClient.getSpace(databaseName).getSpace_id()));
            } catch (TException | ExecuteFailedException e) {
                LOG.error("get spaceId error, ", e);
            }
            return new CatalogDatabaseImpl(props, databaseName);
        } else {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
    }

    /**
     * objectName in tablePath mush start with VERTEX. or EDGE.
     *
     * @param tablePath A graphSpace name and label name.
     * @return
     */
    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        String graphSpace = tablePath.getDatabaseName();
        String table = tablePath.getObjectName();
        try {
            return (listTables(graphSpace).contains(table));
        } catch (DatabaseNotExistException e) {
            throw new CatalogException("failed to call tableExists function, ", e);
        }
    }


    /**
     * show all tags and edges
     *
     * @param graphSpace nebula graph space
     * @return List of Tag and Edge, tag starts with VERTEX. and edge starts with EDGE. .
     */
    @Override
    public List<String> listTables(String graphSpace) throws DatabaseNotExistException,
            CatalogException {
        if (!databaseExists(graphSpace)) {
            throw new DatabaseNotExistException(getName(), graphSpace);
        }

        try {
            metaClient.connect();
        } catch (TException | ClientServerIncompatibleException e) {
            LOG.error("failed to connect meta service vis {} ", address, e);
            throw new CatalogException("nebula meta service connect failed.", e);
        }
        List<String> tables = new ArrayList<>();
        try {
            for (TagItem tag : metaClient.getTags(graphSpace)) {
                tables.add("VERTEX" + NebulaConstant.POINT + tag.tag_name);
            }
            for (EdgeItem edge : metaClient.getEdges(graphSpace)) {
                tables.add("EDGE" + NebulaConstant.POINT + edge.edge_name);
            }
        } catch (TException | ExecuteFailedException e) {
            LOG.error("get tags or edges error,", e);
        }
        return tables;
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException,
            org.apache.flink.table.catalog.exceptions.CatalogException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(getName(), tablePath);
        }

        String graphSpace = tablePath.getDatabaseName();
        String[] typeAndLabel = tablePath.getObjectName().split(NebulaConstant.SPLIT_POINT);
        String type = typeAndLabel[0];
        String label = typeAndLabel[1];
        if (!DataTypeEnum.checkValidDataType(type)) {
            LOG.warn("tablePath does not exist in nebula");
            return null;
        }

        try {
            metaClient.connect();
        } catch (TException | ClientServerIncompatibleException e) {
            LOG.error("failed to connect meta service vis {} ", address, e);
            throw new CatalogException("nebula meta service connect failed.", e);
        }

        Schema schema;
        try {
            if (DataTypeEnum.valueOf(type).isVertex()) {
                schema = metaClient.getTag(graphSpace, label);
            } else {
                schema = metaClient.getEdge(graphSpace, label);
            }
        } catch (TException | ExecuteFailedException e) {
            LOG.error("get tag or edge schema error, ", e);
            return null;
        }

        String[] names = new String[schema.columns.size()];
        DataType[] types = new DataType[schema.columns.size()];
        for (int i = 0; i < schema.columns.size(); i++) {
            names[i] = new String(schema.columns.get(i).getName());
            types[i] = fromNebulaType(schema.columns, i);
        }

        TableSchema.Builder tableBuilder = new TableSchema.Builder()
                .fields(names, types);
        Map<String, String> props = new HashMap<>();
        props.put(CONNECTOR.key(), IDENTIFIER);
        props.put(METAADDRESS.key(), address);
        props.put(USERNAME.key(), username);
        props.put(PASSWORD.key(), password);
        props.put(GRAPH_SPACE.key(), tablePath.getDatabaseName());
        props.put(LABEL_NAME.key(), tablePath.getObjectName());
        TableSchema tableSchema = tableBuilder.build();

        return new CatalogTableImpl(tableSchema, props, "nebulaTableCatalog");
    }


    /**
     * construct flink datatype from nebula type
     *
     * @see PropertyType
     */
    private DataType fromNebulaType(List<ColumnDef> columns, int colIndex) {
        int type = columns.get(colIndex).getType().type.getValue();

        switch (PropertyType.findByValue(type)) {
            case INT8:
            case INT16:
            case INT32:
            case INT64:
            case VID:
                return DataTypes.BIGINT();
            case BOOL:
                return DataTypes.BOOLEAN();
            case FLOAT:
                return DataTypes.FLOAT();
            case DOUBLE:
                return DataTypes.DOUBLE();
            case TIMESTAMP:
                return DataTypes.TIMESTAMP();
            case DATE:
            case TIME:
            case DATETIME:
            case STRING:
            case FIXED_STRING:
                return DataTypes.STRING();
            case UNKNOWN:
                return DataTypes.NULL();
            default:
                throw new UnsupportedOperationException(String.format("Doesn't support nebula "
                        + "type '%s' yet", columns.get(colIndex).getType()));
        }
    }
}
