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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.netty.PartitionRequestClient;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * An input channel, which requests a remote partition queue.
 */
public class RemoteInputChannel extends InputChannel {

	private static final Logger LOG = LoggerFactory.getLogger(RemoteInputChannel.class);

	/** ID to distinguish this channel from other channels sharing the same TCP connection. */
	private final InputChannelID id = new InputChannelID();

	/** The connection to use to request the remote partition. */
	private final ConnectionID connectionId;

	/** The connection manager to use connect to the remote partition provider. */
	private final ConnectionManager connectionManager;

	/**
	 * The received buffers. Received buffers are enqueued by the network I/O thread and the queue
	 * is consumed by the receiving task thread.
	 */
	private final Queue<Buffer> receivedBuffers = new ArrayDeque<Buffer>();

	/**
	 * Flag indicating whether this channel has been released. Either called by the receiving task
	 * thread or the task manager actor.
	 */
	private final AtomicBoolean isReleased = new AtomicBoolean();

	/** Client to establish a (possibly shared) TCP connection and request the partition. */
	private volatile PartitionRequestClient partitionRequestClient;

	/**
	 * The next expected sequence number for the next buffer. This is modified by the network
	 * I/O thread only.
	 */
	private int expectedSequenceNumber = 0;

	public RemoteInputChannel(
			SingleInputGate inputGate,
			int channelIndex,
			ResultPartitionID partitionId,
			ConnectionID connectionId,
			ConnectionManager connectionManager) {

		this(inputGate, channelIndex, partitionId, connectionId, connectionManager,
				new Tuple2<Integer, Integer>(0, 0));
	}

	public RemoteInputChannel(
			SingleInputGate inputGate,
			int channelIndex,
			ResultPartitionID partitionId,
			ConnectionID connectionId,
			ConnectionManager connectionManager,
			Tuple2<Integer, Integer> initialAndMaxBackoff) {

		super(inputGate, channelIndex, partitionId, initialAndMaxBackoff);

		this.connectionId = checkNotNull(connectionId);
		this.connectionManager = checkNotNull(connectionManager);
	}

	// ------------------------------------------------------------------------
	// Consume
	// ------------------------------------------------------------------------

	/**
	 * Requests a remote subpartition.
	 *
	 * 这里把this传进了netty客户端，所以看onBuffer方法
	 *
	 */
	@Override
	void requestSubpartition(int subpartitionIndex) throws IOException, InterruptedException {
		if (partitionRequestClient == null) {


			// Create a client and request the partition
			partitionRequestClient = connectionManager
					.createPartitionRequestClient(connectionId);

			partitionRequestClient.requestSubpartition(partitionId, subpartitionIndex, this, 0);
		}
	}

	/**
	 * Retriggers a remote subpartition request.
	 */
	void retriggerSubpartitionRequest(int subpartitionIndex) throws IOException, InterruptedException {
		checkState(partitionRequestClient != null, "Missing initial subpartition request.");

		if (increaseBackoff()) {
			partitionRequestClient.requestSubpartition(
					partitionId, subpartitionIndex, this, getCurrentBackoff());
		}
		else {
			failPartitionRequest();
		}
	}

	/**
	 *对于每个InputChannel，消费的生命周期会经历如下的方法调用过程：

		 requestSubpartition：请求ResultSubpartition；
		 getNextBuffer：获得下一个Buffer；
		 releaseAllResources：释放所有的相关资源；
	 *
	 *   InputChannel根据ResultPartitionLocation提供了三种实现：

		 LocalInputChannel：用于请求同实例中生产者任务所生产的ResultSubpartitionView的输入通道；
		 RemoteInputChannel：用于请求远程生产者任务所生产的ResultSubpartitionView的输入通道；
		 UnknownInputChannel：一种用于占位目的的输入通道，需要占位通道是因为暂未确定相对于生产者任务位置，
		 但最终要么被替换为RemoteInputChannel，要么被替换为LocalInputChannel。
	 *
	 *   LocalInputChannel会从相同的JVM实例中消费生产者任务所生产的Buffer。因此，这种模式是直接借助于方法调用和对象共享的机制完成消费，
	 *   无需跨节点网络通信。具体而言，它是通过ResultPartitionManager来直接创建对应的ResultSubpartitionView的实例，这种通道相对简单。
	 *
	 *
	 *  远程数据交换的通信机制建立在Netty框架的基础之上，因此会有一个主交互对象PartitionRequestClient来衔接通信层跟输入通道。
	 *
	 *
	 *
	 *
	 * @return
	 * @throws IOException
	 */
	@Override
	Buffer getNextBuffer() throws IOException {
		checkState(!isReleased.get(), "Queried for a buffer after channel has been closed.");
		checkState(partitionRequestClient != null, "Queried for a buffer before requesting a queue.");

		checkError();

		synchronized (receivedBuffers) {
			Buffer buffer = receivedBuffers.poll();

			// Sanity check that channel is only queried after a notification
			if (buffer == null) {
				throw new IOException("Queried input channel for data although non is available.");
			}

			return buffer;
		}
	}

	// ------------------------------------------------------------------------
	// Task events
	// ------------------------------------------------------------------------

	@Override
	void sendTaskEvent(TaskEvent event) throws IOException {
		checkState(!isReleased.get(), "Tried to send task event to producer after channel has been released.");
		checkState(partitionRequestClient != null, "Tried to send task event to producer before requesting a queue.");

		checkError();

		partitionRequestClient.sendTaskEvent(partitionId, event, this);
	}

	// ------------------------------------------------------------------------
	// Life cycle
	// ------------------------------------------------------------------------

	@Override
	boolean isReleased() {
		return isReleased.get();
	}

	@Override
	void notifySubpartitionConsumed() {
		// Nothing to do
	}

	/**
	 * Releases all received buffers and closes the partition request client.
	 */
	@Override
	void releaseAllResources() throws IOException {
		if (isReleased.compareAndSet(false, true)) {
			synchronized (receivedBuffers) {
				Buffer buffer;
				while ((buffer = receivedBuffers.poll()) != null) {
					buffer.recycle();
				}
			}

			// The released flag has to be set before closing the connection to ensure that
			// buffers received concurrently with closing are properly recycled.
			if (partitionRequestClient != null) {
				partitionRequestClient.close(this);
			}
			else {
				connectionManager.closeOpenChannelConnections(connectionId);
			}
		}
	}

	public void failPartitionRequest() {
		setError(new PartitionNotFoundException(partitionId));
	}

	@Override
	public String toString() {
		return "RemoteInputChannel [" + partitionId + " at " + connectionId + "]";
	}

	// ------------------------------------------------------------------------
	// Network I/O notifications (called by network I/O thread)
	// ------------------------------------------------------------------------

	public int getNumberOfQueuedBuffers() {
		synchronized (receivedBuffers) {
			return receivedBuffers.size();
		}
	}

	public InputChannelID getInputChannelId() {
		return id;
	}

	public BufferProvider getBufferProvider() throws IOException {
		if (isReleased.get()) {
			return null;
		}

		return inputGate.getBufferProvider();
	}

	public void onBuffer(Buffer buffer, int sequenceNumber) {
		boolean success = false;

		try {
			synchronized (receivedBuffers) {
				if (!isReleased.get()) {
					//如果实际的序列号跟所期待的序列号相等
					if (expectedSequenceNumber == sequenceNumber) {
						//将数据加入接收队列同时将预期序列号计数器加一
						receivedBuffers.add(buffer);
						expectedSequenceNumber++;
						//发出有可用Buffer的通知，该通知随后会被传递给其所归属的SingleInputGate，
						//以通知其订阅者，有可用数据
						notifyAvailableBuffer();

						success = true;
					}
					else {
						//如果实际序列号跟所期待的序列号不一致，则会触发onError回调，并相应以一个特定的异常对象
						//该方法调用在成功设置完错误原因后，同样会触发notifyAvailableBuffer方法调用
						onError(new BufferReorderingException(expectedSequenceNumber, sequenceNumber));
					}
				}
			}
		}
		finally {
			if (!success) {
				//如果不成功，则该Buffer会被回收
				buffer.recycle();
			}
		}
	}

	public void onEmptyBuffer(int sequenceNumber) {
		synchronized (receivedBuffers) {
			if (!isReleased.get()) {
				if (expectedSequenceNumber == sequenceNumber) {
					expectedSequenceNumber++;
				}
				else {
					onError(new BufferReorderingException(expectedSequenceNumber, sequenceNumber));
				}
			}
		}
	}

	public void onFailedPartitionRequest() {
		inputGate.triggerPartitionStateCheck(partitionId);
	}

	public void onError(Throwable cause) {
		setError(cause);
	}

	public static class BufferReorderingException extends IOException {

		private static final long serialVersionUID = -888282210356266816L;

		private final int expectedSequenceNumber;

		private final int actualSequenceNumber;

		public BufferReorderingException(int expectedSequenceNumber, int actualSequenceNumber) {
			this.expectedSequenceNumber = expectedSequenceNumber;
			this.actualSequenceNumber = actualSequenceNumber;
		}

		@Override
		public String getMessage() {
			return String.format("Buffer re-ordering: expected buffer with sequence number %d, but received %d.",
					expectedSequenceNumber, actualSequenceNumber);
		}
	}
}
