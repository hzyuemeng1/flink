/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;

import java.io.IOException;

public class NettyConnectionManager implements ConnectionManager {

	private final NettyServer server;

	private final NettyClient client;
	/**
	 * NettyBufferPool通过反射还拿到了PooledByteBufAllocator中的PoolArena分配器对象集合，但此举更多的是出于调试目的。
	 */
	private final NettyBufferPool bufferPool;

	private final PartitionRequestClientFactory partitionRequestClientFactory;

	public NettyConnectionManager(NettyConfig nettyConfig) {
		this.server = new NettyServer(nettyConfig);
		this.client = new NettyClient(nettyConfig);
		this.bufferPool = new NettyBufferPool(nettyConfig.getNumberOfArenas());

		this.partitionRequestClientFactory = new PartitionRequestClientFactory(client);
	}


	/**
	 *
	 * 每个PartitionRequestClientFactory实例都依赖一个NettyClient。也就是说所有PartitionRequestClient底层都共用一个NettyClient。
	 *
	 *
	 * @throws IOException
	 */
	@Override
	public void start(ResultPartitionProvider partitionProvider, TaskEventDispatcher taskEventDispatcher, NetworkBufferPool networkbufferPool)
			throws IOException {
		PartitionRequestProtocol partitionRequestProtocol =
				new PartitionRequestProtocol(partitionProvider, taskEventDispatcher, networkbufferPool);

		client.init(partitionRequestProtocol, bufferPool);
		server.init(partitionRequestProtocol, bufferPool);
	}

	@Override
	public PartitionRequestClient createPartitionRequestClient(ConnectionID connectionId)
			throws IOException, InterruptedException {

		/**
		 * 小心炸弹， partitionRequestClientFactory会依赖NettyClient，就在上面
		 */
		return partitionRequestClientFactory.createPartitionRequestClient(connectionId);
	}

	@Override
	public void closeOpenChannelConnections(ConnectionID connectionId) {
		partitionRequestClientFactory.closeOpenChannelConnections(connectionId);
	}

	@Override
	public int getNumberOfActiveConnections() {
		return partitionRequestClientFactory.getNumberOfActiveClients();
	}

	@Override
	public void shutdown() {
		client.shutdown();
		server.shutdown();
	}

	NettyClient getClient() {
		return client;
	}

	NettyServer getServer() {
		return server;
	}

	NettyBufferPool getBufferPool() {
		return bufferPool;
	}
}
