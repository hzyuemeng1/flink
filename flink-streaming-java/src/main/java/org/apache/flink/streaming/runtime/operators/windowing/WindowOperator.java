/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.operators.windowing;

import com.google.common.annotations.VisibleForTesting;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.MergingState;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.Triggerable;
import org.apache.flink.streaming.runtime.operators.windowing.buffers.WindowBufferFactory;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalWindowFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTaskState;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * An operator that implements the logic for windowing based on a {@link WindowAssigner} and
 * {@link Trigger}.
 *
 * <p>
 * When an element arrives it gets assigned a key using a {@link KeySelector} and it get's
 * assigned to zero or more windows using a {@link WindowAssigner}. Based on this the element
 * is put into panes. A pane is the bucket of elements that have the same key and same
 * {@code Window}. An element can be in multiple panes of it was assigned to multiple windows by the
 * {@code WindowAssigner}.
 *
 * <p>
 * Each pane gets its own instance of the provided {@code Trigger}. This trigger determines when
 * the contents of the pane should be processed to emit results. When a trigger fires,
 * the given {@link InternalWindowFunction} is invoked to produce the results that are emitted for
 * the pane to which the {@code Trigger} belongs.
 *
 * <p>
 * This operator also needs a {@link WindowBufferFactory} to create a buffer for storing the
 * elements of each pane.
 *
 * @param <K> The type of key returned by the {@code KeySelector}.
 * @param <IN> The type of the incoming elements.
 * @param <OUT> The type of elements emitted by the {@code InternalWindowFunction}.
 * @param <W> The type of {@code Window} that the {@code WindowAssigner} assigns.
 */
@Internal
public class WindowOperator<K, IN, ACC, OUT, W extends Window>
	extends AbstractUdfStreamOperator<OUT, InternalWindowFunction<ACC, OUT, K, W>>
	implements OneInputStreamOperator<IN, OUT>, Triggerable, InputTypeConfigurable {

	private static final long serialVersionUID = 1L;

	// ------------------------------------------------------------------------
	// Configuration values and user functions
	// ------------------------------------------------------------------------

	protected final WindowAssigner<? super IN, W> windowAssigner;

	protected final KeySelector<IN, K> keySelector;  //这个是由keyStream的得来的

	protected final Trigger<? super IN, ? super W> trigger;

	protected final StateDescriptor<? extends MergingState<IN, ACC>, ?> windowStateDescriptor;

	/**
	 * This is used to copy the incoming element because it can be put into several window
	 * buffers.
	 */
	protected TypeSerializer<IN> inputSerializer;

	/**
	 * For serializing the key in checkpoints.
	 */
	protected final TypeSerializer<K> keySerializer;

	/**
	 * For serializing the window in checkpoints.
	 */
	protected final TypeSerializer<W> windowSerializer;

	// ------------------------------------------------------------------------
	// State that is not checkpointed
	// ------------------------------------------------------------------------

	/**
	 * This is given to the {@code InternalWindowFunction} for emitting elements with a given timestamp.
	 */
	protected transient TimestampedCollector<OUT> timestampedCollector;

	/**
	 * To keep track of the current watermark so that we can immediately fire if a trigger
	 * registers an event time callback for a timestamp that lies in the past.
	 */
	protected transient long currentWatermark = -1L;

	protected transient Context context = new Context(null, null);

	// ------------------------------------------------------------------------
	// State that needs to be checkpointed
	// ------------------------------------------------------------------------

	/**
	 * Processing time timers that are currently in-flight.
	 */
	protected transient Set<Timer<K, W>> processingTimeTimers;
	protected transient PriorityQueue<Timer<K, W>> processingTimeTimersQueue;

	/**
	 * Current waiting watermark callbacks.
	 */
	protected transient Set<Timer<K, W>> watermarkTimers;
	protected transient PriorityQueue<Timer<K, W>> watermarkTimersQueue;

	/**
	 * Creates a new {@code WindowOperator} based on the given policies and user functions.
	 */
	public WindowOperator(WindowAssigner<? super IN, W> windowAssigner,
		TypeSerializer<W> windowSerializer,
		KeySelector<IN, K> keySelector,
		TypeSerializer<K> keySerializer,
		StateDescriptor<? extends MergingState<IN, ACC>, ?> windowStateDescriptor,
		InternalWindowFunction<ACC, OUT, K, W> windowFunction,
		Trigger<? super IN, ? super W> trigger) {

		super(windowFunction);

		this.windowAssigner = requireNonNull(windowAssigner);
		this.windowSerializer = windowSerializer;
		this.keySelector = requireNonNull(keySelector);
		this.keySerializer = requireNonNull(keySerializer);

		this.windowStateDescriptor = windowStateDescriptor;
		this.trigger = requireNonNull(trigger);

		setChainingStrategy(ChainingStrategy.ALWAYS);
	}

	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		in.defaultReadObject();
		currentWatermark = -1;
	}

	@Override
	@SuppressWarnings("unchecked")
	public final void setInputType(TypeInformation<?> type, ExecutionConfig executionConfig) {
		inputSerializer = (TypeSerializer<IN>) type.createSerializer(executionConfig);
	}

	@Override
	public final void open() throws Exception {
		super.open();

		timestampedCollector = new TimestampedCollector<>(output);

		if (inputSerializer == null) {
			throw new IllegalStateException("Input serializer was not set.");
		}

		// these could already be initialized from restoreState()
		if (watermarkTimers == null) {
			watermarkTimers = new HashSet<>();
			watermarkTimersQueue = new PriorityQueue<>(100);
		}
		if (processingTimeTimers == null) {
			processingTimeTimers = new HashSet<>();
			processingTimeTimersQueue = new PriorityQueue<>(100);
		}

		context = new Context(null, null);
	}

	@Override
	public final void close() throws Exception {
		super.close();
	}

	/**
	 * 请看一下它的子类 EvictingWindowOperator
	 *
	 * 关于事件窗口的时间戳和waterMark怎么来的，请看DataStream.assignTimestampsAndWatermarks，这个方法有两个重载
	 * 一个是周期生成water，一个是每次元素来了，自己判断是否要生成watermark，具体可以参考官档。
	 *
	 * @param element
	 * @throws Exception
	 */
	@Override
	@SuppressWarnings("unchecked")
	public void processElement(StreamRecord<IN> element) throws Exception {
		Collection<W> elementWindows = windowAssigner.assignWindows(element.getValue(), element.getTimestamp());

		K key = (K) getStateBackend().getCurrentKey();

		for (W window: elementWindows) {
            //首先拿到窗口state
			MergingState<IN, ACC> windowState = getPartitionedState(window, windowSerializer,
				windowStateDescriptor);

			windowState.add(element.getValue());

			context.key = key;
			context.window = window;
			TriggerResult triggerResult = context.onElement(element);

			processTriggerResult(triggerResult, key, window);
		}
	}

	protected void processTriggerResult(TriggerResult triggerResult, K key, W window) throws Exception {
		if (!triggerResult.isFire() && !triggerResult.isPurge()) {
			// do nothing
			return;
		}

		if (triggerResult.isFire()) {
			timestampedCollector.setAbsoluteTimestamp(window.maxTimestamp());

			MergingState<IN, ACC> windowState = getPartitionedState(window, windowSerializer,
				windowStateDescriptor);

			ACC contents = windowState.get();

			userFunction.apply(context.key, context.window, contents, timestampedCollector);

			if (triggerResult.isPurge()) {

				/**
				 * 当然啦，这个清理不止是清理事件窗口。
				 *
				 * 点进context.clear()，很容易明白
				 *
				 */
				windowState.clear();
				context.clear();
			}
		} else if (triggerResult.isPurge()) {
			MergingState<IN, ACC> windowState = getPartitionedState(window, windowSerializer,
				windowStateDescriptor);
			windowState.clear();
			context.clear();
		}
	}

	@Override
	public final void processWatermark(Watermark mark) throws Exception {
		//定义一个标识，表示是否仍有定时器满足触发条件
		boolean fire;

		do {
			//从水印定时器队列中查找队首的一个定时器，注意此处并不是出队（注意跟remove方法的区别）
			Timer<K, W> timer = watermarkTimersQueue.peek();
			//如果定时器存在，且其时间戳戳不大于水印的时间戳
			//（注意理解条件是：不大于，水印用于表示小于该时间戳的元素都已到达，所以所有不大于水印的触发时间戳都该被触发）
			if (timer != null && timer.timestamp <= mark.getTimestamp()) {
				//置标识为真，表示找到满足触发条件的定时器
				fire = true;
				//将该元素从队首出队
				watermarkTimers.remove(timer);
				watermarkTimersQueue.remove();

				context.key = timer.key;
				context.window = timer.window;
				setKeyContext(timer.key);
				//通过上下文对象调用窗口触发器的事件时间处理方法并获得触发结果对象
				TriggerResult triggerResult = context.onEventTime(timer.timestamp);
				processTriggerResult(triggerResult, context.key, context.window);
			} else {
				fire = false;
			}
		} while (fire);
		//向下游发射水印
		output.emitWatermark(mark);
		//将当前算子的水印属性用新水印的时间戳覆盖
		this.currentWatermark = mark.getTimestamp();
	}

	@Override
	public final void trigger(long time) throws Exception {
		boolean fire;

		do {
			Timer<K, W> timer = processingTimeTimersQueue.peek();
			if (timer != null && timer.timestamp <= time) {
				fire = true;

				processingTimeTimers.remove(timer);
				processingTimeTimersQueue.remove();

				context.key = timer.key;
				context.window = timer.window;
				setKeyContext(timer.key);
				TriggerResult triggerResult = context.onProcessingTime(timer.timestamp);
				processTriggerResult(triggerResult, context.key, context.window);
			} else {
				fire = false;
			}
		} while (fire);

		// Also check any watermark timers. We might have some in here since
		// Context.registerEventTimeTimer sets a trigger if an event-time trigger is registered
		// that is already behind the watermark.
		processWatermark(new Watermark(currentWatermark));
	}

	/**
	 * {@code Context} is a utility for handling {@code Trigger} invocations. It can be reused
	 * by setting the {@code key} and {@code window} fields. No internal state must be kept in
	 * the {@code Context}
	 */
	protected class Context implements TriggerContext {
		protected K key;
		protected W window;

		public Context(K key, W window) {
			this.key = key;
			this.window = window;
		}

		public long getCurrentWatermark() {
			return currentWatermark;
		}

		@Override
		public <S extends Serializable> ValueState<S> getKeyValueState(String name,
			Class<S> stateType,
			S defaultState) {
			requireNonNull(stateType, "The state type class must not be null");

			TypeInformation<S> typeInfo;
			try {
				typeInfo = TypeExtractor.getForClass(stateType);
			}
			catch (Exception e) {
				throw new RuntimeException("Cannot analyze type '" + stateType.getName() +
					"' from the class alone, due to generic type parameters. " +
					"Please specify the TypeInformation directly.", e);
			}

			return getKeyValueState(name, typeInfo, defaultState);
		}

		@Override
		public <S extends Serializable> ValueState<S> getKeyValueState(String name,
			TypeInformation<S> stateType,
			S defaultState) {

			requireNonNull(name, "The name of the state must not be null");
			requireNonNull(stateType, "The state type information must not be null");

			ValueStateDescriptor<S> stateDesc = new ValueStateDescriptor<>(name, stateType.createSerializer(getExecutionConfig()), defaultState);
			return getPartitionedState(stateDesc);
		}

		@SuppressWarnings("unchecked")
		public <S extends State> S getPartitionedState(StateDescriptor<S, ?> stateDescriptor) {
			try {
				return WindowOperator.this.getPartitionedState(window, windowSerializer, stateDescriptor);
			} catch (Exception e) {
				throw new RuntimeException("Could not retrieve state", e);
			}
		}

		@Override
		public void registerProcessingTimeTimer(long time) {
			//注意定时器的equ方法
			Timer<K, W> timer = new Timer<>(time, key, window);
			if (processingTimeTimers.add(timer)) {
				processingTimeTimersQueue.add(timer);
				getRuntimeContext().registerTimer(time, WindowOperator.this);
			}
		}

		/**
		 * 特别的注意，这里和上面那个，定时器的机制
		 *
		 *
		 * @param time The watermark at which to invoke {@link Trigger#onEventTime(long, Window, TriggerContext)}
		 */
		@Override
		public void registerEventTimeTimer(long time) {
			Timer<K, W> timer = new Timer<>(time, key, window);
			if (watermarkTimers.add(timer)) {
				watermarkTimersQueue.add(timer);
			}
			/**
			 *
			 * 这句话的意思挺有意思
			 */
			if (time <= currentWatermark) {
				// immediately schedule a trigger, so that we don't wait for the next
				// watermark update to fire the watermark trigger
				getRuntimeContext().registerTimer(System.currentTimeMillis(), WindowOperator.this);
			}
		}

		@Override
		public void deleteProcessingTimeTimer(long time) {
			Timer<K, W> timer = new Timer<>(time, key, window);
			if (processingTimeTimers.remove(timer)) {
				processingTimeTimersQueue.remove(timer);
			}
		}

		@Override
		public void deleteEventTimeTimer(long time) {
			Timer<K, W> timer = new Timer<>(time, key, window);
			if (watermarkTimers.remove(timer)) {
				watermarkTimersQueue.remove(timer);
			}

		}

		public TriggerResult onElement(StreamRecord<IN> element) throws Exception {
			return trigger.onElement(element.getValue(), element.getTimestamp(), window, this);
		}

		public TriggerResult onProcessingTime(long time) throws Exception {
			return trigger.onProcessingTime(time, window, this);
		}

		public TriggerResult onEventTime(long time) throws Exception {
			return trigger.onEventTime(time, window, this);
		}

		public void clear() throws Exception {
			trigger.clear(window, this);
		}

		@Override
		public String toString() {
			return "Context{" +
				"key=" + key +
				", window=" + window +
				'}';
		}
	}

	/**
	 * Internal class for keeping track of in-flight timers.
	 */
	protected static class Timer<K, W extends Window> implements Comparable<Timer<K, W>> {
		protected long timestamp;
		protected K key;
		protected W window;

		public Timer(long timestamp, K key, W window) {
			this.timestamp = timestamp;
			this.key = key;
			this.window = window;
		}

		@Override
		public int compareTo(Timer<K, W> o) {
			return Long.compare(this.timestamp, o.timestamp);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()){
				return false;
			}

			Timer<?, ?> timer = (Timer<?, ?>) o;

			return timestamp == timer.timestamp
				&& key.equals(timer.key)
				&& window.equals(timer.window);

		}

		@Override
		public int hashCode() {
			int result = (int) (timestamp ^ (timestamp >>> 32));
			result = 31 * result + key.hashCode();
			result = 31 * result + window.hashCode();
			return result;
		}

		@Override
		public String toString() {
			return "Timer{" +
				"timestamp=" + timestamp +
				", key=" + key +
				", window=" + window +
				'}';
		}
	}

	// ------------------------------------------------------------------------
	//  Checkpointing
	// ------------------------------------------------------------------------

	@Override
	public StreamTaskState snapshotOperatorState(long checkpointId, long timestamp) throws Exception {
		StreamTaskState taskState = super.snapshotOperatorState(checkpointId, timestamp);

		AbstractStateBackend.CheckpointStateOutputView out =
			getStateBackend().createCheckpointStateOutputView(checkpointId, timestamp);

		out.writeInt(watermarkTimersQueue.size());
		for (Timer<K, W> timer : watermarkTimersQueue) {
			keySerializer.serialize(timer.key, out);
			windowSerializer.serialize(timer.window, out);
			out.writeLong(timer.timestamp);
		}

		out.writeInt(processingTimeTimers.size());
		for (Timer<K, W> timer : processingTimeTimersQueue) {
			keySerializer.serialize(timer.key, out);
			windowSerializer.serialize(timer.window, out);
			out.writeLong(timer.timestamp);
		}

		taskState.setOperatorState(out.closeAndGetHandle());

		return taskState;
	}

	@Override
	public void restoreState(StreamTaskState taskState, long recoveryTimestamp) throws Exception {
		super.restoreState(taskState, recoveryTimestamp);

		final ClassLoader userClassloader = getUserCodeClassloader();

		@SuppressWarnings("unchecked")
		StateHandle<DataInputView> inputState = (StateHandle<DataInputView>) taskState.getOperatorState();
		DataInputView in = inputState.getState(userClassloader);

		int numWatermarkTimers = in.readInt();
		watermarkTimers = new HashSet<>(numWatermarkTimers);
		watermarkTimersQueue = new PriorityQueue<>(Math.max(numWatermarkTimers, 1));
		for (int i = 0; i < numWatermarkTimers; i++) {
			K key = keySerializer.deserialize(in);
			W window = windowSerializer.deserialize(in);
			long timestamp = in.readLong();
			Timer<K, W> timer = new Timer<>(timestamp, key, window);
			watermarkTimers.add(timer);
			watermarkTimersQueue.add(timer);
		}

		int numProcessingTimeTimers = in.readInt();
		processingTimeTimers = new HashSet<>(numProcessingTimeTimers);
		processingTimeTimersQueue = new PriorityQueue<>(Math.max(numProcessingTimeTimers, 1));
		for (int i = 0; i < numProcessingTimeTimers; i++) {
			K key = keySerializer.deserialize(in);
			W window = windowSerializer.deserialize(in);
			long timestamp = in.readLong();
			Timer<K, W> timer = new Timer<>(timestamp, key, window);
			processingTimeTimers.add(timer);
			processingTimeTimersQueue.add(timer);
		}
	}

	// ------------------------------------------------------------------------
	// Getters for testing
	// ------------------------------------------------------------------------

	@VisibleForTesting
	public Trigger<? super IN, ? super W> getTrigger() {
		return trigger;
	}

	@VisibleForTesting
	public KeySelector<IN, K> getKeySelector() {
		return keySelector;
	}

	@VisibleForTesting
	public WindowAssigner<? super IN, W> getWindowAssigner() {
		return windowAssigner;
	}

	@VisibleForTesting
	public StateDescriptor<? extends MergingState<IN, ACC>, ?> getStateDescriptor() {
		return windowStateDescriptor;
	}
}
