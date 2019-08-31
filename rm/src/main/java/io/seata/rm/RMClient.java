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
package io.seata.rm;

import io.seata.core.rpc.netty.RmMessageListener;
import io.seata.core.rpc.netty.RmRpcClient;

/**
 * The Rm client Initiator.
 *
 * @author jimin.jm @alibaba-inc.com
 */
public class RMClient {

    /**
     * Init.初始化RmRpcClient
     *
     * @param applicationId           the application id
     * @param transactionServiceGroup the transaction service group
     */
    public static void init(String applicationId, String transactionServiceGroup) {
        // 初始化rm
        RmRpcClient rmRpcClient = RmRpcClient.getInstance(applicationId, transactionServiceGroup);
        // 资源管理器(会将实现委托给实现类，AT or TCC)
        rmRpcClient.setResourceManager(DefaultResourceManager.get());
        // 资源提交、回滚、删除undo log的处理
        rmRpcClient.setClientMessageListener(new RmMessageListener(DefaultRMHandler.get()));
        // 开启netty和定时任务
        rmRpcClient.init();
    }

}
