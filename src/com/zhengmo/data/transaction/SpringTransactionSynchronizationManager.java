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

import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * {@link SynchronizationManager} delegating calls to Spring's {@link TransactionSynchronizationManager}.
 * 
 * @author Michael Hunger
 * @author Oliver Gierke
 * @since 1.6
 */
enum SpringTransactionSynchronizationManager implements SynchronizationManager {
    /**
     * 实例.
     */
    INSTANCE;
    /*
     * (non-Javadoc)
     * @see org.springframework.data.transaction.SynchronizationManager#clearSynchronization()
     */
    @Override
    public void clearSynchronization() {
        TransactionSynchronizationManager.clear();
    }
    /*
     * (non-Javadoc)
     * @see org.springframework.data.transaction.SynchronizationManager#initSynchronization()
     */
    @Override
    public void initSynchronization() {
        TransactionSynchronizationManager.initSynchronization();
    }
    /*
     * (non-Javadoc)
     * @see org.springframework.data.transaction.SynchronizationManager#isSynchronizationActive()
     */
    @Override
    public boolean isSynchronizationActive() {
        return TransactionSynchronizationManager.isSynchronizationActive();
    }
}
