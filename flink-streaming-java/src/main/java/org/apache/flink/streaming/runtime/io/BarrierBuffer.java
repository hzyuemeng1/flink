/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.io;

import java.io.IOException;
import java.util.ArrayDeque;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.util.event.EventListener;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The barrier buffer is {@link CheckpointBarrierHandler} that blocks inputs with barriers until
 * all inputs have received the barrier for a given checkpoint.
 * 
 * <p>To avoid back-pressuring the input streams (which may cause distributed deadlocks), the
 * BarrierBuffer continues receiving buffers from the blocked channels and stores them internally until 
 * the blocks are released.</p>
 */
@Internal
public class BarrierBuffer implements CheckpointBarrierHandler {

	private static final Logger LOG = LoggerFactory.getLogger(BarrierBuffer.class);
	
	/** The gate that the buffer draws its input from */
	private final InputGate inputGate;

	/** Flags that indicate whether a channel is currently blocked/buffered */
	private final boolean[] blockedChannels;
	
	/** The total number of channels that this buffer handles data from */
	private final int totalNumberOfInputChannels;
	
	/** To utility to write blocked data to a file channel */
	private final BufferSpiller bufferSpiller;

	/** The pending blocked buffer/event sequences. Must be consumed before requesting
	 * further data from the input gate. */
	private final ArrayDeque<BufferSpiller.SpilledBufferOrEventSequence> queuedBuffered;

	/** The sequence of buffers/events that has been unblocked and must now be consumed
	 * before requesting further data from the input gate */
	private BufferSpiller.SpilledBufferOrEventSequence currentBuffered;

	/** Handler that receives the checkpoint notifications */
	private EventListener<CheckpointBarrier> checkpointHandler;

	/** The ID of the checkpoint for which we expect barriers */
	private long currentCheckpointId = -1L;

	/** The number of received barriers (= number of blocked/buffered channels) */
	private int numBarriersReceived;
	
	/** The number of already closed channels */
	private int numClosedChannels;
	
	/** Flag to indicate whether we have drawn all available input */
	private boolean endOfStream;

	/**
	 * 
	 * @param inputGate The input gate to draw the buffers and events from.
	 * @param ioManager The I/O manager that gives access to the temp directories.
	 * 
	 * @throws IOException Thrown, when the spilling to temp files cannot be initialized.
	 */
	public BarrierBuffer(InputGate inputGate, IOManager ioManager) throws IOException {
		this.inputGate = inputGate;
		this.totalNumberOfInputChannels = inputGate.getNumberOfInputChannels();
		this.blockedChannels = new boolean[this.totalNumberOfInputChannels];
		
		this.bufferSpiller = new BufferSpiller(ioManager, inputGate.getPageSize());
		this.queuedBuffered = new ArrayDeque<BufferSpiller.SpilledBufferOrEventSequence>();
	}

	// ------------------------------------------------------------------------
	//  Buffer and barrier handling
	// ------------------------------------------------------------------------

	/**
	 * 接口有两个实现，这个实现是对齐，另一个是不用对齐
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	public BufferOrEvent getNextNonBlocked() throws IOException, InterruptedException {
		while (true) {
			// process buffered BufferOrEvents before grabbing new ones
			//获得下一个待缓存的buffer或者barrier事件
			BufferOrEvent next;
			//如果当前的缓冲区为null，则从输入端获得
			if (currentBuffered == null) {

				/**
				 * inputGate的来源
				 * org/apache/flink/runtime/taskmanager/Task.java:300
				 *
				 */
				next = inputGate.getNextBufferOrEvent();
			}
			//如果缓冲区不为空，则从缓冲区中获得数据
			else {
				next = currentBuffered.getNext();
				//如果获得的数据为null，则表示缓冲区中已经没有更多地数据了
				if (next == null) {
					//清空当前缓冲区，获取已经新的缓冲区并打开它
					completeBufferedSequence();
					//递归调用，处理下一条数据
					return getNextNonBlocked();
				}
			}

			//获取到一条记录，不为null
			if (next != null) {
				//如果获取到得记录所在的channel已经处于阻塞状态，则该记录会被加入缓冲区
				if (isBlocked(next.getChannelIndex())) {
					// if the channel is blocked we, we just store the BufferOrEvent
					bufferSpiller.add(next);
				}
				//如果该记录是一个正常的记录，而不是一个barrier(事件)，则直接返回
				else if (next.isBuffer()) {
					return next;
				}
				//如果是一个barrier
				else if (next.getEvent().getClass() == CheckpointBarrier.class) {
					//并且当前流还未处于结束状态，则处理该barrier
					if (!endOfStream) {
						// process barriers only if there is a chance of the checkpoint completing
						processBarrier((CheckpointBarrier) next.getEvent(), next.getChannelIndex());
					}
				}
				else {
					//如果它是一个事件，表示当前已到达分区末尾
					if (next.getEvent().getClass() == EndOfPartitionEvent.class) {
						//以关闭的channel计数器加一
						numClosedChannels++;
						// no chance to complete this checkpoint
						//此时已经没有机会完成该检查点，则解除阻塞
						releaseBlocks();
					}
					//返回该事件
					return next;
				}
			}
			//next 为null 同时流结束标识为false
			else if (!endOfStream) {
				// end of stream. we feed the data that is still buffered
				//置流结束标识为true
				endOfStream = true;
				//解除阻塞，这种情况下我们会看到，缓冲区的数据会被加入队列，并等待处理
				releaseBlocks();
				//继续获取下一个待处理的记录
				return getNextNonBlocked();
			}
			else {
				return null;
			}
		}
	}
	
	private void completeBufferedSequence() throws IOException {
		currentBuffered.cleanup();
		currentBuffered = queuedBuffered.pollFirst();
		if (currentBuffered != null) {
			currentBuffered.open();
		}
	}
	
	private void processBarrier(CheckpointBarrier receivedBarrier, int channelIndex) throws IOException {
		final long barrierId = receivedBarrier.getId();
        //获取接收到得barrier的ID
       //接收到的barrier数目 > 0 ，说明当前正在处理某个检查点的过程中
		if (numBarriersReceived > 0) {
			//当前检查点的某个后续的barrierId
			// subsequent barrier of a checkpoint.
			if (barrierId == currentCheckpointId) {
				//处理barrier
				// regular case
				onBarrier(channelIndex);
			}
			else if (barrierId > currentCheckpointId) {
				// we did not complete the current checkpoint
				LOG.warn("Received checkpoint barrier for checkpoint {} before completing current checkpoint {}. " +
						"Skipping current checkpoint.", barrierId, currentCheckpointId);
         //当前的检查点已经没有机会完成了，则解除阻塞
				releaseBlocks();
				currentCheckpointId = barrierId;
				onBarrier(channelIndex);
			}
			else {
				// ignore trailing barrier from aborted checkpoint
				return;
			}
			
		}//接收到的barrier数目等于0且barrierId > currentCheckpointId
		else if (barrierId > currentCheckpointId) {
			//说明这是一个新检查点的初始barrier
			// first barrier of a new checkpoint
			currentCheckpointId = barrierId;
			onBarrier(channelIndex);
		}
		else {
			//忽略之前（跳过的）检查点的未处理的barrier
			// trailing barrier from previous (skipped) checkpoint
			return;
		}


		//接收到barriers的数目 + 关闭的channel的数目 = 输入channel的总数目
		// check if we have all barriers
		if (numBarriersReceived + numClosedChannels == totalNumberOfInputChannels) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Received all barrier, triggering checkpoint {} at {}",
						receivedBarrier.getId(), receivedBarrier.getTimestamp());
			}
          //触发检查点处理器回调事件
			if (checkpointHandler != null) {
				checkpointHandler.onEvent(receivedBarrier);
			}
			
			releaseBlocks();
		}
	}
	
	@Override
	public void registerCheckpointEventHandler(EventListener<CheckpointBarrier> checkpointHandler) {
		if (this.checkpointHandler == null) {
			this.checkpointHandler = checkpointHandler;
		}
		else {
			throw new IllegalStateException("BarrierBuffer already has a registered checkpoint handler");
		}
	}
	
	@Override
	public boolean isEmpty() {
		return currentBuffered == null;
	}

	@Override
	public void cleanup() throws IOException {
		bufferSpiller.close();
		if (currentBuffered != null) {
			currentBuffered.cleanup();
		}
		for (BufferSpiller.SpilledBufferOrEventSequence seq : queuedBuffered) {
			seq.cleanup();
		}
	}
	
	/**
	 * Checks whether the channel with the given index is blocked.
	 * 
	 * @param channelIndex The channel index to check.
	 * @return True if the channel is blocked, false if not.
	 */
	private boolean isBlocked(int channelIndex) {
		return blockedChannels[channelIndex];
	}
	
	/**
	 * Blocks the given channel index, from which a barrier has been received.
	 *
	 *
	 * 将barrier关联的channel标识为阻塞状态同时将barrier计数器加一
	 * @param channelIndex The channel index to block.
	 */
	private void onBarrier(int channelIndex) throws IOException {
		if (!blockedChannels[channelIndex]) {
			blockedChannels[channelIndex] = true;
			numBarriersReceived++;
			
			if (LOG.isDebugEnabled()) {
				LOG.debug("Received barrier from channel " + channelIndex);
			}
		}
		else {
			throw new IOException("Stream corrupt: Repeated barrier for same checkpoint and input stream");
		}
	}

	/**
	 * Releases the blocks on all channels. Makes sure the just written data
	 * is the next to be consumed.
	 *
	 * 解除所有channel的阻塞，并确保刚刚写入的数据（buffer）被消费
	 */
	private void releaseBlocks() throws IOException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Releasing blocks");
		}

		for (int i = 0; i < blockedChannels.length; i++) {
			//将所有channel的阻塞标识置为false
			blockedChannels[i] = false;
		}
		///将接收到的barrier累加值重置为0
		numBarriersReceived = 0;
		//如果当前的缓冲区中的数据为空
		if (currentBuffered == null) {
			//初始化新的缓冲区读写器
			// common case: no more buffered data
			currentBuffered = bufferSpiller.rollOver();
			if (currentBuffered != null) {
				currentBuffered.open();
			}
		}
		else {
			// uncommon case: buffered data pending
			// push back the pending data, if we have any
			//缓冲区中还有数据，则初始化一块新的存储空间来存储新的缓冲数据
			// since we did not fully drain the previous sequence, we need to allocate a new buffer for this one
			BufferSpiller.SpilledBufferOrEventSequence bufferedNow = bufferSpiller.rollOverWithNewBuffer();
			if (bufferedNow != null) {
				bufferedNow.open();
				queuedBuffered.addFirst(currentBuffered);
				//将新开辟的缓冲区读写器置为新的当前缓冲区
				currentBuffered = bufferedNow;
			}
		}
	}

	// ------------------------------------------------------------------------
	// For Testing
	// ------------------------------------------------------------------------

	/**
	 * Gets the ID defining the current pending, or just completed, checkpoint.
	 * 
	 * @return The ID of the pending of completed checkpoint. 
	 */
	public long getCurrentCheckpointId() {
		return this.currentCheckpointId;
	}
	
	// ------------------------------------------------------------------------
	// Utilities 
	// ------------------------------------------------------------------------
	
	@Override
	public String toString() {
		return String.format("last checkpoint: %d, current barriers: %d, closed channels: %d",
				currentCheckpointId, numBarriersReceived, numClosedChannels);
	}
}
