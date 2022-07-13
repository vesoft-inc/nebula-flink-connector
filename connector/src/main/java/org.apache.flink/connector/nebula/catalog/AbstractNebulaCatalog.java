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
                                 String pwd, String address) {
        super(catalogName, defaultDatabase == null ? DEFAULT_DATABASE : defaultDatabase);
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(username),
                "username cannot be null or empty.");
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(pwd),
                "password cannot be null or empty.");
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(address),
                "address cannot be null or empty."
        );
        this.username = username;
        this.password = pwd;
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
    public boolean databaseExists(String graphName) throws CatalogException {
        return listDatabases().contains(graphName);
    }

    @Override
    public void createDatabase(String s, CatalogDatabase catalogDatabase, boolean b)
            throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropDatabase(String s, boolean b, boolean b1) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterDatabase(String s, CatalogDatabase catalogDatabase, boolean b)
            throws CatalogException {
        throw new UnsupportedOperationException();
    }

    /**
     * operator for nebula graph tag and edge, parameter should be graphName.tag or graphName.edge
     */
    @Override
    public List<String> listViews(String graphName) throws CatalogException {
        return Collections.emptyList();
    }


    @Override
    public void dropTable(ObjectPath objectPath, boolean b) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameTable(ObjectPath objectPath, String s, boolean b) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createTable(ObjectPath objectPath, CatalogBaseTable catalogBaseTable, boolean b)
            throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterTable(ObjectPath objectPath, CatalogBaseTable catalogBaseTable, boolean b)
            throws CatalogException {
        throw new UnsupportedOperationException();
    }

    /**
     * operator for partition
     */
    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath objectPath)
            throws CatalogException {
        return Collections.emptyList();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath objectPath,
                                                     CatalogPartitionSpec catalogPartitionSpec)
            throws CatalogException {
        return Collections.emptyList();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(ObjectPath objectPath,
                                                             List<Expression> list)
            throws CatalogException {
        return Collections.emptyList();
    }

    @Override
    public CatalogPartition getPartition(ObjectPath objectPath,
                                         CatalogPartitionSpec catalogPartitionSpec)
            throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean partitionExists(ObjectPath objectPath,
                                   CatalogPartitionSpec catalogPartitionSpec)
            throws CatalogException {
        return false;
    }

    @Override
    public void createPartition(ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec,
                                CatalogPartition catalogPartition, boolean b)
            throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropPartition(ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec,
                              boolean b) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartition(ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec,
                               CatalogPartition catalogPartition, boolean b)
            throws CatalogException {
        throw new UnsupportedOperationException();
    }

    /**
     * operator for function
     */
    @Override
    public List<String> listFunctions(String s) throws CatalogException {
        return Collections.emptyList();
    }

    @Override
    public CatalogFunction getFunction(ObjectPath objectPath) throws FunctionNotExistException,
            CatalogException {
        throw new FunctionNotExistException(getName(), objectPath);
    }

    @Override
    public boolean functionExists(ObjectPath objectPath) throws CatalogException {
        return false;
    }

    @Override
    public void createFunction(ObjectPath objectPath, CatalogFunction catalogFunction, boolean b)
            throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterFunction(ObjectPath objectPath, CatalogFunction catalogFunction, boolean b)
            throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropFunction(ObjectPath objectPath, boolean b) throws CatalogException {

    }

    /**
     * operator for stat
     */
    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath objectPath)
            throws CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath objectPath)
            throws CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(ObjectPath objectPath,
                                                         CatalogPartitionSpec catalogPartitionSpec)
            throws CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(
            ObjectPath objectPath,
            CatalogPartitionSpec catalogPartitionSpec)
            throws CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public void alterTableStatistics(ObjectPath objectPath,
                                     CatalogTableStatistics catalogTableStatistics, boolean b)
            throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterTableColumnStatistics(ObjectPath objectPath,
                                           CatalogColumnStatistics catalogColumnStatistics,
                                           boolean b) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartitionStatistics(ObjectPath objectPath,
                                         CatalogPartitionSpec catalogPartitionSpec,
                                         CatalogTableStatistics catalogTableStatistics,
                                         boolean b) throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartitionColumnStatistics(ObjectPath objectPath,
                                               CatalogPartitionSpec catalogPartitionSpec,
                                               CatalogColumnStatistics catalogColumnStatistics,
                                               boolean b) throws CatalogException {
        throw new UnsupportedOperationException();
    }
}
