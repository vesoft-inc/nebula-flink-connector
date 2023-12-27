/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.connection;

import com.facebook.thrift.TException;
import com.vesoft.nebula.PropertyType;
import com.vesoft.nebula.client.graph.data.CASignedSSLParam;
import com.vesoft.nebula.client.graph.data.HostAddress;
import com.vesoft.nebula.client.graph.data.SSLParam;
import com.vesoft.nebula.client.graph.data.SelfSignedSSLParam;
import com.vesoft.nebula.client.graph.exception.ClientServerIncompatibleException;
import com.vesoft.nebula.client.meta.MetaClient;
import com.vesoft.nebula.client.meta.exception.ExecuteFailedException;
import com.vesoft.nebula.meta.ColumnDef;
import com.vesoft.nebula.meta.Schema;
import com.vesoft.nebula.meta.SpaceItem;
import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.connector.nebula.utils.VidTypeEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NebulaMetaConnectionProvider implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(NebulaMetaConnectionProvider.class);
    private static final long serialVersionUID = -1045337416133033961L;

    private final NebulaClientOptions nebulaClientOptions;

    public NebulaMetaConnectionProvider(NebulaClientOptions nebulaClientOptions) {
        this.nebulaClientOptions = nebulaClientOptions;
    }

    public MetaClient getMetaClient() throws TException, ClientServerIncompatibleException,
            UnknownHostException {
        List<HostAddress> addresses = nebulaClientOptions.getMetaAddress();
        int timeout = nebulaClientOptions.getTimeout();
        int retry = nebulaClientOptions.getConnectRetry();
        MetaClient metaClient;
        if (nebulaClientOptions.isEnableMetaSSL()) {
            switch (nebulaClientOptions.getSSLSignType()) {
                case CA: {
                    CASignParams caSignParams = nebulaClientOptions.getCaSignParam();
                    SSLParam sslParam = new CASignedSSLParam(caSignParams.getCaCrtFilePath(),
                            caSignParams.getCrtFilePath(), caSignParams.getKeyFilePath());
                    metaClient = new MetaClient(addresses, timeout, retry, retry, true, sslParam);
                    break;
                }
                case SELF: {
                    SelfSignParams selfSignParams = nebulaClientOptions.getSelfSignParam();
                    SSLParam sslParam = new SelfSignedSSLParam(selfSignParams.getCrtFilePath(),
                            selfSignParams.getKeyFilePath(), selfSignParams.getPassword());
                    metaClient = new MetaClient(addresses, timeout, retry, retry, true, sslParam);
                    break;
                }
                default:
                    throw new IllegalArgumentException("ssl sign type is not supported.");
            }
        } else {
            metaClient = new MetaClient(addresses, timeout, retry, retry);
        }

        metaClient.setVersion(nebulaClientOptions.getVersion());
        metaClient.connect();
        return metaClient;
    }

    /**
     * get Nebula Graph vid type
     *
     * @param space nebula graph space
     * @return {@link VidTypeEnum}
     */
    public VidTypeEnum getVidType(MetaClient metaClient, String space) {
        SpaceItem spaceItem;
        try {
            spaceItem = metaClient.getSpace(space);
        } catch (TException | ExecuteFailedException e) {
            LOG.error("get space info error, ", e);
            throw new RuntimeException(e);
        }
        PropertyType vidType = spaceItem.getProperties().getVid_type().getType();
        if (vidType == PropertyType.FIXED_STRING) {
            return VidTypeEnum.STRING;
        } else {
            return VidTypeEnum.INT;
        }
    }

    /**
     * get schema info for tag
     *
     * @param space nebula graph space
     * @param tag   nebula graph tag
     * @return Map property name -> {@link PropertyType}
     */
    public Map<String, Integer> getTagSchema(MetaClient metaClient, String space, String tag) {
        Map<String, Integer> schema = new HashMap<>();
        Schema tagSchema;
        try {
            tagSchema = metaClient.getTag(space, tag);
        } catch (TException | ExecuteFailedException e) {
            LOG.error("get tag schema error, ", e);
            throw new RuntimeException(e);
        }
        List<ColumnDef> columnDefs = tagSchema.getColumns();
        for (ColumnDef col : columnDefs) {
            schema.put(new String(col.getName()), col.getType().getType().getValue());
        }
        return schema;
    }

    /**
     * get schema info for edge
     *
     * @param space nebula graph space
     * @param edge  nebula graph edge
     * @return Map property name -> {@link PropertyType}
     */
    public Map<String, Integer> getEdgeSchema(MetaClient metaClient, String space, String edge) {
        Map<String, Integer> schema = new HashMap<>();
        Schema edgeSchema;
        try {
            edgeSchema = metaClient.getEdge(space, edge);
        } catch (TException | ExecuteFailedException e) {
            LOG.error("get edge schema error, ", e);
            throw new RuntimeException(e);
        }
        List<ColumnDef> columnDefs = edgeSchema.getColumns();
        for (ColumnDef col : columnDefs) {
            schema.put(new String(col.getName()), col.getType().getType().getValue());
        }
        return schema;
    }
}
