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

package org.apache.flink.runtime.io.network.api.reader;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.accumulators.AccumulatorRegistry;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer.DeserializationResult;
import org.apache.flink.runtime.io.network.api.serialization.SpillingAdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;

import java.io.IOException;

/**
 * A record-oriented reader.
 * <p>
 * This abstract base class is used by both the mutable and immutable record readers.
 *
 * @param <T> The type of the record that can be read with this record reader.
 */
abstract class AbstractRecordReader<T extends IOReadableWritable> extends AbstractReader implements ReaderBase {

	private final RecordDeserializer<T>[] recordDeserializers;

	private RecordDeserializer<T> currentRecordDeserializer;

	private boolean isFinished;

	@SuppressWarnings("unchecked")
	protected AbstractRecordReader(InputGate inputGate) {
		super(inputGate);

		// Initialize one deserializer per input channel
		this.recordDeserializers = new SpillingAdaptiveSpanningRecordDeserializer[inputGate.getNumberOfInputChannels()];
		for (int i = 0; i < recordDeserializers.length; i++) {
			recordDeserializers[i] = new SpillingAdaptiveSpanningRecordDeserializer<T>();
		}
	}

	/**
	 * 可变记录读取器和不可变记录读取器的差别是：不可变记录读取器的getNextRecord方法的record参数是其内部实例化的而可变记录读取器中
	 * 该引用是外部提供的。事实上，Flink源码中没有找到可变记录读取器（RecordReader）的使用场景。
	 *
	 *
	 * @param target
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	protected boolean getNextRecord(T target) throws IOException, InterruptedException {
		if (isFinished) {
			return false;
		}

		while (true) {
			//如果当前反序列化器已被初始化，说明它当前正在序列化一个记录
			if (currentRecordDeserializer != null) {
				//以当前反序列化器对记录进行反序列化，并返回反序列化结果枚举DeserializationResult
				DeserializationResult result = currentRecordDeserializer.getNextRecord(target);

				//如果获得结果是当前的Buffer已被消费（还不是记录的完整结果），获得当前的Buffer，将其回收，
				//后续会继续反序列化当前记录的剩余数据
				if (result.isBufferConsumed()) {
					final Buffer currentBuffer = currentRecordDeserializer.getCurrentBuffer();

					currentBuffer.recycle();
					currentRecordDeserializer = null;
				}
				//如果结果表示记录已被完全消费，则返回true，跳出循环
				if (result.isFullRecord()) {
					return true;
				}
			}
			//从输入闸门获得下一个Buffer或者事件对象
			final BufferOrEvent bufferOrEvent = inputGate.getNextBufferOrEvent();

			if (bufferOrEvent.isBuffer()) {
				//设置当前的反序列化器，并将当前记录对应的Buffer给反序列化器
				currentRecordDeserializer = recordDeserializers[bufferOrEvent.getChannelIndex()];
				currentRecordDeserializer.setNextBuffer(bufferOrEvent.getBuffer());
			}
			else {
				//如果不是Buffer而是事件，则根据其对应的通道索引拿到对应的反序列化器判断其是否还有未完成的数据，
				//如果有则抛出异常，因为这是一个新的事件，在处理它之前，反序列化器中不应该存在残留数据
				// sanity check for leftover data in deserializers. events should only come between
				// records, not in the middle of a fragment
				if (recordDeserializers[bufferOrEvent.getChannelIndex()].hasUnfinishedData()) {
					throw new IOException(
							"Received an event in channel " + bufferOrEvent.getChannelIndex() + " while still having "
							+ "data from a record. This indicates broken serialization logic. "
							+ "If you are using custom serialization code (Writable or Value types), check their "
							+ "serialization routines. In the case of Kryo, check the respective Kryo serializer.");
				}
				//处理事件，当该事件表示分区的子分区消费完成或者超步整体结束
				if (handleEvent(bufferOrEvent.getEvent())) {
					//如果是整个ResultPartition都消费完成
					if (inputGate.isFinished()) {
						isFinished = true;
						return false;
					} //否则判断如果到达超步尾部
					else if (hasReachedEndOfSuperstep()) {
						return false;
					}
					// else: More data is coming...
				}
			}
		}
	}

	public void clearBuffers() {
		for (RecordDeserializer<?> deserializer : recordDeserializers) {
			Buffer buffer = deserializer.getCurrentBuffer();
			if (buffer != null && !buffer.isRecycled()) {
				buffer.recycle();
			}
		}
	}

	@Override
	public void setReporter(AccumulatorRegistry.Reporter reporter) {
		for (RecordDeserializer<?> deserializer : recordDeserializers) {
			deserializer.setReporter(reporter);
		}
	}
}
