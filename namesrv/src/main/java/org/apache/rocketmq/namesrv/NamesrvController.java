/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.namesrv;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.Configuration;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.namesrv.kvconfig.KVConfigManager;
import org.apache.rocketmq.namesrv.processor.ClusterTestRequestProcessor;
import org.apache.rocketmq.namesrv.processor.DefaultRequestProcessor;
import org.apache.rocketmq.namesrv.routeinfo.BrokerHousekeepingService;
import org.apache.rocketmq.namesrv.routeinfo.RouteInfoManager;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.netty.NettyRemotingServer;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NamesrvController {
	private static final Logger log = LoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

	/*
	 * zl07499 Namesrv相关参数
	 */
	private final NamesrvConfig namesrvConfig;

	/**
	 * zl07499 Netty相关参数
	 */
	private final NettyServerConfig nettyServerConfig;

	/**
	 * zl07499
	 * 单线程池，此线程用来启动namesrc，启动之后还有2个定时线程来scanNotActiveBroker（清理不生效broker）和printAllPeriodically
	 */
	private final ScheduledExecutorService scheduledExecutorService = Executors
			.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("NSScheduledThread"));

	/**
	 * zl07499 namesrv配置管理器
	 */
	private final KVConfigManager kvConfigManager;

	/**
	 * zl07499
	 * 所有运行数据管理器，topicqueuetable、brokeraddrtable等，信息量很多，ReadWriteLock保证读写安全
	 */
	private final RouteInfoManager routeInfoManager;

	/**
	 * zl07499 服务启动接口，这里传入的是NettyRemotingServer，用netty启动，是rocketmq
	 * remoting模块，包括注册请求处理器DefaultRequestProcessor，以及几种数据传输方式invokeSync、invokeAsync、invokeOneway等
	 */
	private RemotingServer remotingServer;

	/**
	 * zl07499 Broker事件监听器，属于netty概念，监听chanel 4个动作事件，提供处理方法
	 */
	private BrokerHousekeepingService brokerHousekeepingService;

	/**
	 * zl07499 remotingServer的并发处理器，处理各种类型请求
	 */
	private ExecutorService remotingExecutor;

	/**
	 * zl07499
	 * 公告类，这里是namesrv和nettyserver2个配置文件，ReadWriteLock保证读写，对外提供获取及持久化，DataVersion（时间戳+AtomicLong
	 * counter自增）做版本控制，Properties对象做update、string-file中间对象。
	 */
	private Configuration configuration;

	public NamesrvController(NamesrvConfig namesrvConfig, NettyServerConfig nettyServerConfig) {
		this.namesrvConfig = namesrvConfig;
		this.nettyServerConfig = nettyServerConfig;
		this.kvConfigManager = new KVConfigManager(this);
		this.routeInfoManager = new RouteInfoManager();
		this.brokerHousekeepingService = new BrokerHousekeepingService(this);
		this.configuration = new Configuration(log, this.namesrvConfig, this.nettyServerConfig);
		this.configuration.setStorePathFromConfig(this.namesrvConfig, "configStorePath");
	}

	public boolean initialize() {

		/**
		 * zl07499 加载kvConfig.json至KVConfigManager的configTable，即持久化转移到内存
		 */
		this.kvConfigManager.load();

		/**
		 * zl07499 将namesrv作为一个netty server启动(用于监听，接收并返回消息)
		 */
		this.remotingServer = new NettyRemotingServer(this.nettyServerConfig, this.brokerHousekeepingService);

		/**
		 * zl07499 启动请求处理线程池
		 */
		this.remotingExecutor = Executors.newFixedThreadPool(nettyServerConfig.getServerWorkerThreads(),
				new ThreadFactoryImpl("RemotingExecutorThread_"));

		/**
		 * zl07499 注册默认DefaultRequestProcessor和remotingExecutor，等start启动即开始处理netty请求
		 * 用于处理netty的消息
		 */
		this.registerProcessor();

		/**
		 * zl07499 每隔10s，判断broker是否存活，不存活则移除
		 */
		this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

			@Override
			public void run() {
				NamesrvController.this.routeInfoManager.scanNotActiveBroker();
			}
		}, 5, 10, TimeUnit.SECONDS);

		/**
		 * 每隔10min，打印所有的K-V
		 */
		this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

			@Override
			public void run() {
				NamesrvController.this.kvConfigManager.printAllPeriodically();
			}
		}, 1, 10, TimeUnit.MINUTES);

		return true;
	}

	private void registerProcessor() {
		if (namesrvConfig.isClusterTest()) {

			this.remotingServer.registerDefaultProcessor(
					new ClusterTestRequestProcessor(this, namesrvConfig.getProductEnvName()), this.remotingExecutor);
		} else {

			this.remotingServer.registerDefaultProcessor(new DefaultRequestProcessor(this), this.remotingExecutor);
		}
	}

	public void start() throws Exception {
		this.remotingServer.start();
	}

	public void shutdown() {
		this.remotingServer.shutdown();
		this.remotingExecutor.shutdown();
		this.scheduledExecutorService.shutdown();
	}

	public NamesrvConfig getNamesrvConfig() {
		return namesrvConfig;
	}

	public NettyServerConfig getNettyServerConfig() {
		return nettyServerConfig;
	}

	public KVConfigManager getKvConfigManager() {
		return kvConfigManager;
	}

	public RouteInfoManager getRouteInfoManager() {
		return routeInfoManager;
	}

	public RemotingServer getRemotingServer() {
		return remotingServer;
	}

	public void setRemotingServer(RemotingServer remotingServer) {
		this.remotingServer = remotingServer;
	}

	public Configuration getConfiguration() {
		return configuration;
	}
}
