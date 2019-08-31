/*
 *  Copyright 1999-2019 Seata.io Group.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package io.seata.rm.datasource;

import java.sql.Connection;
import java.sql.SQLException;

import com.alibaba.druid.util.JdbcConstants;

import io.seata.config.ConfigurationFactory;
import io.seata.core.constants.ConfigurationKeys;
import io.seata.core.exception.TransactionException;
import io.seata.core.exception.TransactionExceptionCode;
import io.seata.core.model.BranchStatus;
import io.seata.core.model.BranchType;
import io.seata.rm.DefaultResourceManager;
import io.seata.rm.datasource.exec.LockConflictException;
import io.seata.rm.datasource.undo.SQLUndoLog;
import io.seata.rm.datasource.undo.UndoLogManager;
import io.seata.rm.datasource.undo.UndoLogManagerOracle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Connection proxy.
 *
 * @author sharajava
 */
public class ConnectionProxy extends AbstractConnectionProxy {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionProxy.class);

    // 保存xid、branchId、锁、资源信息
    private ConnectionContext context = new ConnectionContext();

    private static final int DEFAULT_REPORT_RETRY_COUNT = 5;

    private static int REPORT_RETRY_COUNT = ConfigurationFactory.getInstance().getInt(
        ConfigurationKeys.CLIENT_REPORT_RETRY_COUNT, DEFAULT_REPORT_RETRY_COUNT);

    /**
     * Instantiates a new Connection proxy.
     *
     * @param dataSourceProxy  the data source proxy
     * @param targetConnection the target connection
     */
    public ConnectionProxy(DataSourceProxy dataSourceProxy, Connection targetConnection) {
        super(dataSourceProxy, targetConnection);
    }

    /**
     * Gets context.
     *
     * @return the context
     */
    public ConnectionContext getContext() {
        return context;
    }

    /**
     * Bind.
     *
     * @param xid the xid
     */
    public void bind(String xid) {
        context.bind(xid);
    }

    /**
     * set global lock requires flag
     *
     * @param isLock whether to lock
     */
    public void setGlobalLockRequire(boolean isLock) {
        context.setGlobalLockRequire(isLock);
    }

    /**
     * get global lock requires flag
     */
    public boolean isGlobalLockRequire() {
        return context.isGlobalLockRequire();
    }

    /**
     * Check lock.
     *
     * @param lockKeys the lockKeys
     * @throws SQLException the sql exception
     */
    public void checkLock(String lockKeys) throws SQLException {
        // Just check lock without requiring lock by now.
        try {
            // 检查可获取锁
            boolean lockable = DefaultResourceManager.get().lockQuery(BranchType.AT,
                getDataSourceProxy().getResourceId(), context.getXid(), lockKeys);
            if (!lockable) {
                throw new LockConflictException();
            }
        } catch (TransactionException e) {
            recognizeLockKeyConflictException(e);
        }
    }

    /**
     * Register.
     *
     * @param lockKeys the lockKeys
     * @throws SQLException the sql exception
     */
    public void register(String lockKeys) throws SQLException {
        // Just check lock without requiring lock by now.
        try {
            DefaultResourceManager.get().branchRegister(BranchType.AT, getDataSourceProxy().getResourceId(), null,
                context.getXid(), null, lockKeys);
        } catch (TransactionException e) {
            recognizeLockKeyConflictException(e);
        }
    }

    private void recognizeLockKeyConflictException(TransactionException te) throws SQLException {
        if (te.getCode() == TransactionExceptionCode.LockKeyConflict) {
            throw new LockConflictException();
        } else {
            throw new SQLException(te);
        }

    }

    /**
     * append sqlUndoLog
     *
     * @param sqlUndoLog the sql undo log
     */
    public void appendUndoLog(SQLUndoLog sqlUndoLog) {
        context.appendUndoItem(sqlUndoLog);
    }

    /**
     * append lockKey
     *
     * @param lockKey the lock key
     */
    public void appendLockKey(String lockKey) {
        context.appendLockKey(lockKey);
    }

    @Override
    public void commit() throws SQLException {
        if (context.inGlobalTransaction()) {
            // 处理全局事务提交。注册分支事务，写入undo log，提交本地, 报告一阶段完成
            processGlobalTransactionCommit();
        } else if (context.isGlobalLockRequire()) {
            // 检查是否可以获取全局锁，可以则提交本地事务
            processLocalCommitWithGlobalLocks();
        } else {
            targetConnection.commit();
        }
    }

    private void processLocalCommitWithGlobalLocks() throws SQLException {
        // 检查锁资源
        checkLock(context.buildLockKeys());
        try {
            targetConnection.commit();
        } catch (Throwable ex) {
            throw new SQLException(ex);
        }
        context.reset();
    }

    // 处理全局事务的提交
    private void processGlobalTransactionCommit() throws SQLException {
        try {
            // 注册分支事务
            register();
        } catch (TransactionException e) {
            // 封装异常并抛出
            recognizeLockKeyConflictException(e);
        }

        // 写入undo log
        try {
            // 数据库写入undo log
            if (context.hasUndoLog()) {
                if (JdbcConstants.ORACLE.equalsIgnoreCase(this.getDbType())) {
                    UndoLogManagerOracle.flushUndoLogs(this);
                } else {
                    UndoLogManager.flushUndoLogs(this);
                }
            }
            // 提交本地事务
            targetConnection.commit();
        } catch (Throwable ex) {
            LOGGER.error("process connectionProxy commit error: {}", ex.getMessage(), ex);
            report(false);
            throw new SQLException(ex);
        }
        // 汇报TC该分支事务一阶段实现成功
        report(true);
        context.reset();
    }

    // 发送分支事务注册请求，并设置返回的branchId
    private void register() throws TransactionException {
        Long branchId = DefaultResourceManager.get().branchRegister(BranchType.AT, getDataSourceProxy().getResourceId(),
            null, context.getXid(), null, context.buildLockKeys());
        context.setBranchId(branchId);
    }

    // 回滚本地事务，并向TC汇报PhaseOne_Failed
    @Override
    public void rollback() throws SQLException {
        // 本地事务回滚
        targetConnection.rollback();
        if (context.inGlobalTransaction()) {
            if (context.isBranchRegistered()) {
                // 如果注册过分支事务，则汇报一阶段失败 PhaseOne_Failed
                report(false);
            }
        }
        // 清除资源
        context.reset();
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        if ((autoCommit) && !getAutoCommit()) {
            // change autocommit from false to true, we should commit() first according to JDBC spec.
            commit();
        }
        targetConnection.setAutoCommit(autoCommit);
    }

    // 向TC汇报一阶段结果
    private void report(boolean commitDone) throws SQLException {
        int retry = REPORT_RETRY_COUNT;
        while (retry > 0) {
            try {
                // 向TC汇报一阶段结果
                DefaultResourceManager.get().branchReport(BranchType.AT, context.getXid(), context.getBranchId(),
                    (commitDone ? BranchStatus.PhaseOne_Done : BranchStatus.PhaseOne_Failed), null);
                return;
            } catch (Throwable ex) {
                LOGGER.error("Failed to report [" + context.getBranchId() + "/" + context.getXid() + "] commit done ["
                    + commitDone + "] Retry Countdown: " + retry);
                retry--;

                // 重试多次依然失败，抛出异常
                if (retry == 0) {
                    throw new SQLException("Failed to report branch status " + commitDone, ex);
                }
            }
        }
    }
}
