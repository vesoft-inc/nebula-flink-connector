/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.catalog;

import static org.apache.flink.util.Preconditions.checkArgument;

import com.facebook.thrift.TException;
import com.vesoft.nebula.client.graph.data.HostAddress;
import com.vesoft.nebula.client.graph.exception.ClientServerIncompatibleException;
import com.vesoft.nebula.client.meta.MetaClient;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.flink.connector.nebula.table.NebulaDynamicTableFactory;
import org.apache.flink.connector.nebula.utils.NebulaUtils;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * AbstractNebulaCatalog is used to get nebula schema
 * flink-nebula catalog doesn't support write operators， such as create/alter
 */
public abstract class AbstractNebulaCatalog extends AbstractCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractNebulaCatalog.class);

    protected final String username;
    protected final String password;
    protected final String address;
    private static final String DEFAULT_DATABASE = "default";

    public AbstractNebulaCatalog(String catalogName, String defaultDatabase, String username,
                                 String password, String address) {
        super(catalogName, defaultDatabase == null ? DEFAULT_DATABASE : defaultDatabase);
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(username),
                "username cannot be null or empty.");
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(password),
                "password cannot be null or empty.");
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(address),
                "address cannot be null or empty."
        );
        this.username = username;
        this.password = password;
        this.address = address;
    }

    @Override
    public void open() throws CatalogException {
        // test metaClient connection
        List<HostAddress> hostAndPorts = NebulaUtils.getHostAndPorts(address);
        MetaClient metaClient = null;
        try {
            metaClient = new MetaClient(hostAndPorts);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException("address is illegal, ", e);
        }
        try {
            metaClient.connect();
            metaClient.close();
        } catch (TException | ClientServerIncompatibleException e) {
            throw new ValidationException(String.format("Failed connecting to meta service via "
                    + "%s, ", address), e);
        }
        LOG.info("Catalog {} established connection to {}", getName(), address);
    }

    @Override
    public void close() throws CatalogException {
        LOG.info("Catalog {} closing", getName());
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getAddress() {
        return address;
    }

    @Override
    public Optional<Factory> getFactory() {
        return Optional.of(new NebulaDynamicTableFactory());
    }

    /**
     * operators for nebula graph
     */
    @Override
    public boolean databaseExists(String dataBaseName) throws CatalogException {
        return listDatabases().contains(dataBaseName);
    }

    @Override
    public void createDatabase(String dataBaseName,
                               CatalogDatabase catalogDatabase,
                               boolean ignoreIfExists) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropDatabase(String dataBaseName, boolean ignoreIfNotExists)
            throws CatalogException {
        dropDatabase(dataBaseName, ignoreIfNotExists, false);
    }

    @Override
    public void dropDatabase(String dataBaseName, boolean ignoreIfNotExists, boolean cascade)
            throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterDatabase(String dataBaseName, CatalogDatabase newDatabase,
                              boolean ignoreIfNotExists) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    /**
     * operator for nebula graph tag and edge, parameter should be databaseName.tag or
     * databaseName.edge
     */
    @Override
    public List<String> listViews(String databaseName) throws CatalogException {
        return Collections.emptyList();
    }

    @Override
    public void dropTable(ObjectPath tablePath,boolean ignoreIfNotExists) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
            throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
            throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterTable(
            ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
            throws CatalogException {
        throw new UnsupportedOperationException();
    }

    /**
     * operator for partition
     */
    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
            throws CatalogException {
        return Collections.emptyList();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath,
                                                     CatalogPartitionSpec partitionSpec)
            throws CatalogException {
        return Collections.emptyList();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(ObjectPath tablePath,
                                                             List<Expression> filters)
            throws CatalogException {
        return Collections.emptyList();
    }

    @Override
    public CatalogPartition getPartition(ObjectPath tablePath,
                                         CatalogPartitionSpec partitionSpec)
            throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean partitionExists(ObjectPath tablePath,
                                   CatalogPartitionSpec partitionSpec)
            throws CatalogException {
        return false;
    }

    @Override
    public void createPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition partition,
            boolean ignoreIfExists) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropPartition(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists)
            throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition newPartition,
            boolean ignoreIfNotExists) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    /**
     * operator for function
     */
    @Override
    public List<String> listFunctions(String dbName) throws CatalogException {
        return Collections.emptyList();
    }

    @Override
    public CatalogFunction getFunction(ObjectPath functionPath) throws FunctionNotExistException,
            CatalogException {
        throw new FunctionNotExistException(getName(), functionPath);
    }

    @Override
    public boolean functionExists(ObjectPath functionPath) throws CatalogException {
        return false;
    }

    @Override
    public void createFunction(
            ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists)
            throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterFunction(
            ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists)
            throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropFunction(
            ObjectPath functionPath, boolean ignoreIfNotExists) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    /**
     * operator for stat
     */
    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath) throws CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
            throws CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(ObjectPath tablePath,
                                                         CatalogPartitionSpec partitionSpec)
            throws CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec)
            throws CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public void alterTableStatistics(
            ObjectPath tablePath,
            CatalogTableStatistics tableStatistics,
            boolean ignoreIfNotExists) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterTableColumnStatistics(
            ObjectPath tablePath,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartitionStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogTableStatistics partitionStatistics,
            boolean ignoreIfNotExists) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartitionColumnStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists) throws CatalogException {
        throw new UnsupportedOperationException();
    }
}
