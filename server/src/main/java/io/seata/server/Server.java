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
package io.seata.server;

import io.seata.common.XID;
import io.seata.common.thread.NamedThreadFactory;
import io.seata.common.util.NetUtil;
import io.seata.core.constants.ConfigurationKeys;
import io.seata.core.rpc.netty.RpcServer;
import io.seata.core.rpc.netty.ShutdownHook;
import io.seata.server.coordinator.DefaultCoordinator;
import io.seata.server.metrics.MetricsManager;
import io.seata.server.session.SessionHolder;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * The type Server.
 *
 * @author jimin.jm @alibaba-inc.com
 */
public class Server {

    private static final int MIN_SERVER_POOL_SIZE = 100;
    private static final int MAX_SERVER_POOL_SIZE = 500;
    private static final int MAX_TASK_QUEUE_SIZE = 20000;
    private static final int KEEP_ALIVE_TIME = 500;
    private static final ThreadPoolExecutor WORKING_THREADS = new ThreadPoolExecutor(MIN_SERVER_POOL_SIZE,
        MAX_SERVER_POOL_SIZE, KEEP_ALIVE_TIME, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(MAX_TASK_QUEUE_SIZE),
        new NamedThreadFactory("ServerHandlerThread", MAX_SERVER_POOL_SIZE), new ThreadPoolExecutor.CallerRunsPolicy());

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     * @throws IOException the io exception
     */
    public static void main(String[] args) throws IOException {
        //initialize the metrics
        MetricsManager.get().init();

        //initialize the parameter parser
        ParameterParser parameterParser = new ParameterParser(args);

        System.setProperty(ConfigurationKeys.STORE_MODE, parameterParser.getStoreMode());
        // 创建netty serverBootstrap
        RpcServer rpcServer = new RpcServer(WORKING_THREADS);
        // 设置netty的属性
        //server host
        rpcServer.setHost(parameterParser.getHost());
        //server port
        rpcServer.setListenPort(parameterParser.getPort());
        UUIDGenerator.init(parameterParser.getServerNode());
        //log store mode : file、db
        // 初始化session存储组件
        SessionHolder.init(parameterParser.getStoreMode());

        // 事务协调组件
        DefaultCoordinator coordinator = new DefaultCoordinator(rpcServer);
        // 初始化后台任务（异步提交、回滚重试、提交重试、超时处理、undoLog删除）
        coordinator.init();
        rpcServer.setHandler(coordinator);
        // register ShutdownHook
        ShutdownHook.getInstance().addDisposable(coordinator);

        //127.0.0.1 and 0.0.0.0 are not valid here.
        // 设置ip和port
        if (NetUtil.isValidIp(parameterParser.getHost(), false)) {
            XID.setIpAddress(parameterParser.getHost());
        } else {
            // 根据NetworkInterface获取本地ip
            XID.setIpAddress(NetUtil.getLocalIp());
        }
        XID.setPort(rpcServer.getListenPort());
        // 初始化handler并开启netty
        rpcServer.init();

        System.exit(0);
    }
}
