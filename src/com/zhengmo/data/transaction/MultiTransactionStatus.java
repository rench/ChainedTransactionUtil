/*
 * Copyright 2011-2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.zhengmo.data.transaction;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionStatus;
import org.springframework.util.Assert;

/**
 * {@link TransactionStatus} implementation to orchestrate {@link TransactionStatus} instances for multiple {@link PlatformTransactionManager} instances.
 * 
 * @author Michael Hunger
 * @author Oliver Gierke
 * @since 1.6
 */
class MultiTransactionStatus implements TransactionStatus {
    /**
     * 内部类.
     * @author zhengmo
     */
    private static class SavePoints {
        /**
         * 事务回滚点.
         */
        private final Map<TransactionStatus, Object> savepoints = new HashMap<TransactionStatus, Object>();
        /**
         * 释放事务点.
         */
        public void release() {
            for (TransactionStatus transactionStatus : savepoints.keySet()) {
                transactionStatus.releaseSavepoint(savepointFor(transactionStatus));
            }
        }
        /**
         * 回滚事务.
         */
        public void rollback() {
            for (TransactionStatus transactionStatus : savepoints.keySet()) {
                transactionStatus.rollbackToSavepoint(savepointFor(transactionStatus));
            }
        }
        /**
         * 添加事务点.
         * @param status 事务状态
         * @param savepoint savepoint
         */
        private void addSavePoint(TransactionStatus status, Object savepoint) {
            Assert.notNull(status, "TransactionStatus must not be null!");
            this.savepoints.put(status, savepoint);
        }
        /**
         * 保存.
         * @param transactionStatus 事务状态
         */
        private void save(TransactionStatus transactionStatus) {
            Object savepoint = transactionStatus.createSavepoint();
            addSavePoint(transactionStatus, savepoint);
        }
        /**
         * 获取事务点.
         * @param transactionStatus 事务状态
         * @return 事务
         */
        private Object savepointFor(TransactionStatus transactionStatus) {
            return savepoints.get(transactionStatus);
        }
    }
    /**
     * 事务管理器.
     */
    private final PlatformTransactionManager mainTransactionManager;
    /**
     * 事务管理器与事务状态.
     */
    private final Map<PlatformTransactionManager, TransactionStatus> transactionStatuses = Collections.synchronizedMap(new HashMap<PlatformTransactionManager, TransactionStatus>());
    /**
     * 是否同步.
     */
    private boolean newSynchonization;
    /**
     * Creates a new {@link MultiTransactionStatus} for the given {@link PlatformTransactionManager}.
     * 
     * @param mainTransactionManager must not be {@literal null}.
     */
    public MultiTransactionStatus(PlatformTransactionManager mainTransactionManager) {
        Assert.notNull(mainTransactionManager, "TransactionManager must not be null!");
        this.mainTransactionManager = mainTransactionManager;
    }
    /**
     * 单个事务提交.
     * @param transactionManager 事务管理器
     */
    public void commit(PlatformTransactionManager transactionManager) {
        TransactionStatus transactionStatus = getTransactionStatus(transactionManager);
        transactionManager.commit(transactionStatus);
    }
    /*
     * (non-Javadoc)
     * @see org.springframework.transaction.SavepointManager#createSavepoint()
     */
    @Override
    public Object createSavepoint() throws TransactionException {
        SavePoints savePoints = new SavePoints();
        for (TransactionStatus transactionStatus : transactionStatuses.values()) {
            savePoints.save(transactionStatus);
        }
        return savePoints;
    }
    /*
     * (non-Javadoc)
     * @see org.springframework.transaction.TransactionStatus#flush()
     */
    @Override
    public void flush() {
        for (TransactionStatus transactionStatus : transactionStatuses.values()) {
            transactionStatus.flush();
        }
    }
    /**
     * 获取事务管理器与状态.
     * @return 集合
     */
    public Map<PlatformTransactionManager, TransactionStatus> getTransactionStatuses() {
        return transactionStatuses;
    }
    /*
     * (non-Javadoc)
     * @see org.springframework.transaction.TransactionStatus#hasSavepoint()
     */
    @Override
    public boolean hasSavepoint() {
        return getMainTransactionStatus().hasSavepoint();
    }
    /*
     * (non-Javadoc)
     * @see org.springframework.transaction.TransactionStatus#isCompleted()
     */
    @Override
    public boolean isCompleted() {
        return getMainTransactionStatus().isCompleted();
    }
    /**
     * get.
     * @return boolean
     */
    public boolean isNewSynchonization() {
        return newSynchonization;
    }
    /*
     * (non-Javadoc)
     * @see org.springframework.transaction.TransactionStatus#isNewTransaction()
     */
    @Override
    public boolean isNewTransaction() {
        return getMainTransactionStatus().isNewTransaction();
    }
    /*
     * (non-Javadoc)
     * @see org.springframework.transaction.TransactionStatus#isRollbackOnly()
     */
    @Override
    public boolean isRollbackOnly() {
        return getMainTransactionStatus().isRollbackOnly();
    }
    /**
     * 注册事务.
     * @param definition 申明
     * @param transactionManager 事务管理器
     */
    public void registerTransactionManager(TransactionDefinition definition, PlatformTransactionManager transactionManager) {
        getTransactionStatuses().put(transactionManager, transactionManager.getTransaction(definition));
    }
    /*
     * (non-Javadoc)
     * @see org.springframework.transaction.SavepointManager#releaseSavepoint(java.lang.Object)
     */
    @Override
    public void releaseSavepoint(Object savepoint) throws TransactionException {
        ((SavePoints) savepoint).release();
    }
    /**
     * Rolls back the {@link TransactionStatus} registered for the given {@link PlatformTransactionManager}.
     * 
     * @param transactionManager must not be {@literal null}.
     */
    public void rollback(PlatformTransactionManager transactionManager) {
        transactionManager.rollback(getTransactionStatus(transactionManager));
    }
    /*
     * (non-Javadoc)
     * @see org.springframework.transaction.SavepointManager#rollbackToSavepoint(java.lang.Object)
     */
    @Override
    public void rollbackToSavepoint(Object savepoint) throws TransactionException {
        SavePoints savePoints = (SavePoints) savepoint;
        savePoints.rollback();
    }
    /**
     * set.
     */
    public void setNewSynchonization() {
        this.newSynchonization = true;
    }
    /*
     * (non-Javadoc)
     * @see org.springframework.transaction.TransactionStatus#setRollbackOnly()
     */
    @Override
    public void setRollbackOnly() {
        for (TransactionStatus ts : transactionStatuses.values()) {
            ts.setRollbackOnly();
        }
    }
    /**
     * 获取主事务状态.
     * @return 事务状态
     */
    private TransactionStatus getMainTransactionStatus() {
        return transactionStatuses.get(mainTransactionManager);
    }
    /**
     * 获取指定事务管理器事务状态.
     * @param transactionManager 事务管理器
     * @return 事务状态
     */
    private TransactionStatus getTransactionStatus(PlatformTransactionManager transactionManager) {
        return this.getTransactionStatuses().get(transactionManager);
    }
}
