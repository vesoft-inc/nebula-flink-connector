/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.sink;

import com.vesoft.nebula.driver.graph.ErrorCode;
import com.vesoft.nebula.driver.graph.data.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.flink.connector.nebula.connection.GraphProvider;
import org.apache.flink.connector.nebula.utils.WriteModeEnum;
import org.slf4j.Logger;

abstract class AbstractNebulaRetryableBatchExecutor<E> {

    protected abstract Logger getLogger();

    protected abstract String getEntityName();

    protected abstract String getWriteTarget();

    protected abstract WriteModeEnum getWriteMode();

    protected abstract boolean throwErrorWhenFailed();

    protected abstract int getRetryTimes();

    protected abstract long getRetryIntervalMs();

    protected abstract String getGql(List<E> entities);

    protected abstract boolean isAlreadyExist(ResultSet resultSet);

    protected abstract long getAffectedCount(ResultSet resultSet);

    protected final String writeEntities(List<E> entities, GraphProvider graphProvider) {
        String statement = getGql(entities);
        ResultSet result;
        try {
            result = graphProvider.execute(statement);
        } catch (Exception e) {
            getLogger().warn("write({}) {} {} failed with exception, now retry writing one by one.",
                             getWriteMode(),
                             getEntityName(),
                             getWriteTarget(),
                             e);
            return writeEntitiesOneByOne(entities, graphProvider);
        }

        if (result.isSucceeded()) {
            getLogger().info(">>>>> batch write({}) for {} succeed. batch size({}), affected({}),"
                                     + " latency({}us)",
                             getWriteMode(),
                             getWriteTarget(),
                             entities.size(),
                             getAffectedCount(result),
                             result.getLatency());
            return null;
        }

        if (entities.size() == 1 && !isRetryable(result)) {
            if (throwErrorWhenFailed()) {
                throw new RuntimeException(String.format(
                        "write(%s) %s %s failed: %s. ngql:\n%s",
                        getWriteMode(),
                        getEntityName(),
                        getWriteTarget(),
                        result.getErrorMessage(),
                        statement));
            }
            getLogger().error("write({}) {} {} failed: {}",
                              getWriteMode(),
                              getEntityName(),
                              getWriteTarget(),
                              result.getErrorMessage());
            return statement;
        }

        getLogger().warn("write({}) {} {} failed: {}, now retry writing one by one.",
                         getWriteMode(),
                         getEntityName(),
                         getWriteTarget(),
                         result.getErrorMessage());
        return writeEntitiesOneByOne(entities, graphProvider);
    }

    private String writeEntitiesOneByOne(List<E> entities, GraphProvider graphProvider) {
        List<String> failedExecs = new ArrayList<>();
        for (E entity : entities) {
            String failedExec = writeEntity(entity, graphProvider);
            if (failedExec != null) {
                failedExecs.add(failedExec);
            }
        }
        return failedExecs.isEmpty() ? null : String.join("; ", failedExecs);
    }

    private String writeEntity(E entity, GraphProvider graphProvider) {
        String statement = getGql(Collections.singletonList(entity));
        ResultSet executeResult;
        try {
            executeResult = graphProvider.execute(statement);
        } catch (Exception e) {
            if (throwErrorWhenFailed()) {
                throw new RuntimeException(String.format(
                        "write(%s) %s %s failed with exception. ngql:\n%s",
                        getWriteMode(),
                        getEntityName(),
                        getWriteTarget(),
                        statement),
                                           e);
            }
            getLogger().error("write({}) {} {} failed with exception.",
                              getWriteMode(),
                              getEntityName(),
                              getWriteTarget(),
                              e);
            return statement;
        }

        if (executeResult.isSucceeded()) {
            getLogger().info("write({}) {} {}, batch size(1), affected({}), latency({}us)",
                             getWriteMode(),
                             getEntityName(),
                             getWriteTarget(),
                             getAffectedCount(executeResult),
                             executeResult.getLatency());
            return null;
        }

        if (isAlreadyExist(executeResult)) {
            getLogger().warn("write {} {} failed, already exists.",
                             getEntityName(),
                             getWriteTarget());
            return null;
        }

        int retry = 0;
        while (retry < getRetryTimes() && isRetryable(executeResult)) {
            retry += 1;
            if (getRetryIntervalMs() > 0) {
                try {
                    Thread.sleep(getRetryIntervalMs());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            try {
                executeResult = graphProvider.execute(statement);
                if (executeResult.isSucceeded()) {
                    getLogger().info(
                            "write({}) {} {}, batch size(1), affected({}), latency({}us)",
                            getWriteMode(),
                            getEntityName(),
                            getWriteTarget(),
                            getAffectedCount(executeResult),
                            executeResult.getLatency());
                    return null;
                }
            } catch (Exception e) {
                getLogger().warn("single {} retry failed with exception, retry={}",
                                 getEntityName(),
                                 retry,
                                 e);
            }
        }

        if (throwErrorWhenFailed()) {
            throw new RuntimeException(String.format(
                    "write(%s) %s %s failed: %s. ngql:\n%s",
                    getWriteMode(),
                    getEntityName(),
                    getWriteTarget(),
                    executeResult.getErrorMessage(),
                    statement));
        }

        getLogger().error("write({}) {} {} failed: {}",
                          getWriteMode(),
                          getEntityName(),
                          getWriteTarget(),
                          executeResult.getErrorMessage());
        return statement;
    }

    private boolean isRetryable(ResultSet resultSet) {
        ErrorCode errorCode = resultSet.getErrorCode();
        return errorCode == ErrorCode.LEADER_CHANGED
                || errorCode.isRpcError()
                || errorCode.isRaftError();
    }
}

