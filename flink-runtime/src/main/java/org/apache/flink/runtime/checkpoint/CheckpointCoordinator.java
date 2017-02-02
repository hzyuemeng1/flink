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

package org.apache.flink.runtime.checkpoint;

import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import akka.actor.Props;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.stats.CheckpointStatsTracker;
import org.apache.flink.runtime.checkpoint.stats.DisabledCheckpointStatsTracker;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.AkkaActorGateway;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.RecoveryMode;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.messages.checkpoint.NotifyCheckpointComplete;
import org.apache.flink.runtime.messages.checkpoint.TriggerCheckpoint;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.util.SerializedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The checkpoint coordinator coordinates the distributed snapshots of operators and state.
 * It triggers the checkpoint by sending the messages to the relevant tasks and collects the
 * checkpoint acknowledgements. It also collects and maintains the overview of the state handles
 * reported by the tasks that acknowledge the checkpoint.
 *
 * <p>Depending on the configured {@link RecoveryMode}, the behaviour of the {@link
 * CompletedCheckpointStore} and {@link CheckpointIDCounter} change. The default standalone
 * implementations don't support any recovery.
 */


public class CheckpointCoordinator {

	static final Logger LOG = LoggerFactory.getLogger(CheckpointCoordinator.class);

	/** The number of recent checkpoints whose IDs are remembered */
	private static final int NUM_GHOST_CHECKPOINT_IDS = 16;

	/** Coordinator-wide lock to safeguard the checkpoint updates */
	protected final Object lock = new Object();

	/** The job whose checkpoint this coordinator coordinates */
	private final JobID job;

	/** Tasks who need to be sent a message when a checkpoint is started */
	private final ExecutionVertex[] tasksToTrigger;//目前只有input ExecutionVertex能够触发checkpoint

	/** Tasks who need to acknowledge a checkpoint before it succeeds */
	private final ExecutionVertex[] tasksToWaitFor;

	/** Tasks who need to be sent a message when a checkpoint is confirmed */
	private final ExecutionVertex[] tasksToCommitTo;

	/** Map from checkpoint ID to the pending checkpoint */
	private final Map<Long, PendingCheckpoint> pendingCheckpoints;//触发checkpoint的时候，会构造pending checkpoint，放入该map

	/** Completed checkpoints. Implementations can be blocking. Make sure calls to methods
	 * accessing this don't block the job manager actor and run asynchronously. */
	private final CompletedCheckpointStore completedCheckpointStore;

	/** A list of recent checkpoint IDs, to identify late messages (vs invalid ones) */
	private final ArrayDeque<Long> recentPendingCheckpoints;

	/** Checkpoint ID counter to ensure ascending IDs. In case of job manager failures, these
	 * need to be ascending across job managers. */
	protected final CheckpointIDCounter checkpointIdCounter;

	/** Class loader used to deserialize the state handles (as they may be user-defined) */
	private final ClassLoader userClassLoader;

	/** The base checkpoint interval. Actual trigger time may be affected by the
	 * max concurrent checkpoints and minimum-pause values */
	private final long baseInterval;//触发checkpoint的间隔，实际这个时间会受到最大并行checkpoint数和最小暂停时间的影响

	/** The max time (in ms) that a checkpoint may take */
	private final long checkpointTimeout;//checkpoint超时时间

	/** The min time(in ms) to delay after a checkpoint could be triggered. Allows to
	 * enforce minimum processing time between checkpoint attempts */
	private final long minPauseBetweenCheckpointsNanos;//两个checkpoint之间最小的等待时间

	/** The maximum number of checkpoints that may be in progress at the same time */
	private final int maxConcurrentCheckpointAttempts;//最大并行checkpoint执行个数在同一时间

	/** The timer that handles the checkpoint timeouts and triggers periodic checkpoints */
	private final Timer timer;//控制checkpoint超时及触发周期checkpoint

	/** Actor that receives status updates from the execution graph this coordinator works for */
	private ActorGateway jobStatusListener;//job状态监听器，主要用于启动和停止checkpoint周期调度器

	/** The number of consecutive failed trigger attempts */
	private int numUnsuccessfulCheckpointsTriggers;//连续失败的checkpoint个数

	private ScheduledTrigger currentPeriodicTrigger;//触发调度器

	/** The timestamp (via {@link System#nanoTime()}) when the last checkpoint completed */
	private long lastCheckpointCompletionNanos;//最后一次checkpoint complete的时间

	/** Flag whether a triggered checkpoint should immediately schedule the next checkpoint.
	 * Non-volatile, because only accessed in synchronized scope */
	private boolean periodicScheduling;//标志已经触发的checkpoint是否立即要调度下一次checkpoint

	/** Flag whether a trigger request could not be handled immediately. Non-volatile, because only
	 * accessed in synchronized scope */
	private boolean triggerRequestQueued;//触发请求能不能立即处理

	/** Flag marking the coordinator as shut down (not accepting any messages any more) */
	private volatile boolean shutdown;

	/** Shutdown hook thread to clean up state handles. */
	private final Thread shutdownHook;

	/** Helper for tracking checkpoint statistics  */
	private final CheckpointStatsTracker statsTracker;//checkpoint监听器

	protected final int numberKeyGroups;

	private final Executor executor;

	// --------------------------------------------------------------------------------------------

	public CheckpointCoordinator(
			JobID job,
			long baseInterval,
			long checkpointTimeout,
			int numberKeyGroups,
			ExecutionVertex[] tasksToTrigger,
			ExecutionVertex[] tasksToWaitFor,
			ExecutionVertex[] tasksToCommitTo,
			ClassLoader userClassLoader,
			CheckpointIDCounter checkpointIDCounter,
			CompletedCheckpointStore completedCheckpointStore,
			RecoveryMode recoveryMode,
			Executor executor) {

		this(job, baseInterval, checkpointTimeout, 0L, Integer.MAX_VALUE, numberKeyGroups,
				tasksToTrigger, tasksToWaitFor, tasksToCommitTo,
				userClassLoader, checkpointIDCounter, completedCheckpointStore, recoveryMode,
				new DisabledCheckpointStatsTracker(), executor);
	}

	public CheckpointCoordinator(
			JobID job,
			long baseInterval,
			long checkpointTimeout,
			long minPauseBetweenCheckpoints,
			int maxConcurrentCheckpointAttempts,
			int numberKeyGroups,
			ExecutionVertex[] tasksToTrigger,
			ExecutionVertex[] tasksToWaitFor,
			ExecutionVertex[] tasksToCommitTo,
			ClassLoader userClassLoader,
			CheckpointIDCounter checkpointIDCounter,
			CompletedCheckpointStore completedCheckpointStore,
			RecoveryMode recoveryMode,
			CheckpointStatsTracker statsTracker,
			Executor executor) {

		// Sanity check
		checkArgument(baseInterval > 0, "Checkpoint timeout must be larger than zero");
		checkArgument(checkpointTimeout >= 1, "Checkpoint timeout must be larger than zero");
		checkArgument(minPauseBetweenCheckpoints >= 0, "minPauseBetweenCheckpoints must be >= 0");
		checkArgument(maxConcurrentCheckpointAttempts >= 1, "maxConcurrentCheckpointAttempts must be >= 1");

		// max "in between duration" can be one year - this is to prevent numeric overflows
		if (minPauseBetweenCheckpoints > 365L * 24 * 60 * 60 * 1_000) {
			minPauseBetweenCheckpoints = 365L * 24 * 60 * 60 * 1_000;
			LOG.warn("Reducing minimum pause between checkpoints to " + minPauseBetweenCheckpoints + " ms (1 year)");
		}

		this.job = checkNotNull(job);
		this.baseInterval = baseInterval;
		this.checkpointTimeout = checkpointTimeout;
		this.minPauseBetweenCheckpointsNanos = minPauseBetweenCheckpoints * 1_000_000;
		this.maxConcurrentCheckpointAttempts = maxConcurrentCheckpointAttempts;
		this.tasksToTrigger = checkNotNull(tasksToTrigger);
		this.tasksToWaitFor = checkNotNull(tasksToWaitFor);
		this.tasksToCommitTo = checkNotNull(tasksToCommitTo);
		this.pendingCheckpoints = new LinkedHashMap<Long, PendingCheckpoint>();
		this.completedCheckpointStore = checkNotNull(completedCheckpointStore);
		this.recentPendingCheckpoints = new ArrayDeque<Long>(NUM_GHOST_CHECKPOINT_IDS);
		this.userClassLoader = userClassLoader;

		// Started with the periodic scheduler
		this.checkpointIdCounter = checkNotNull(checkpointIDCounter);

		this.timer = new Timer("Checkpoint Timer", true);

		this.statsTracker = checkNotNull(statsTracker);

		if (recoveryMode == RecoveryMode.STANDALONE) {
			// Add shutdown hook to clean up state handles when no checkpoint recovery is
			// possible. In case of another configured recovery mode, the checkpoints need to be
			// available for the standby job managers.
			this.shutdownHook = new Thread(new Runnable() {
				@Override
				public void run() {
					try {
						CheckpointCoordinator.this.shutdown();
					}
					catch (Throwable t) {
						LOG.error("Error during shutdown of checkpoint coordinator via " +
								"JVM shutdown hook: " + t.getMessage(), t);
					}
				}
			});

			try {
				// Add JVM shutdown hook to call shutdown of service
				Runtime.getRuntime().addShutdownHook(shutdownHook);
			}
			catch (IllegalStateException ignored) {
				// JVM is already shutting down. No need to do anything.
			}
			catch (Throwable t) {
				LOG.error("Cannot register checkpoint coordinator shutdown hook.", t);
			}
		}
		else {
			this.shutdownHook = null;
		}

		this.numberKeyGroups = numberKeyGroups;

		this.executor = checkNotNull(executor);
	}

	// --------------------------------------------------------------------------------------------
	// Callbacks
	// --------------------------------------------------------------------------------------------

	/**
	 * Callback on shutdown of the coordinator. Called in lock scope.
	 */
	protected void onShutdown() {
	}

	/**
	 * Callback on cancellation of a checkpoint. Called in lock scope.
	 */
	protected void onCancelCheckpoint(long canceledCheckpointId) {
	}

	/**
	 * Callback on full acknowledgement of a checkpoint. Called in lock scope.
	 */
	protected void onFullyAcknowledgedCheckpoint(CompletedCheckpoint checkpoint) {
	}

	// --------------------------------------------------------------------------------------------
	//  Clean shutdown
	// --------------------------------------------------------------------------------------------

	/**
	 * Shuts down the checkpoint coordinator.
	 *
	 * <p>After this method has been called, the coordinator does not accept
	 * and further messages and cannot trigger any further checkpoints. All
	 * checkpoint state is discarded.
	 */
	public void shutdown() throws Exception {
		shutdown(true);
	}

	/**
	 * Suspends the checkpoint coordinator.
	 *
	 * <p>After this method has been called, the coordinator does not accept
	 * and further messages and cannot trigger any further checkpoints.
	 *
	 * <p>The difference to shutdown is that checkpoint state in the store
	 * and counter is kept around if possible to recover later.
	 */
	public void suspend() throws Exception {
		shutdown(false);
	}

	/**
	 * Shuts down the checkpoint coordinator.
	 *
	 * @param shutdownStoreAndCounter Depending on this flag the checkpoint
	 * state services are shut down or suspended.
	 */
	private void shutdown(boolean shutdownStoreAndCounter) throws Exception {
		synchronized (lock) {
			try {
				if (!shutdown) {
					shutdown = true;
					LOG.info("Stopping checkpoint coordinator for job " + job);

					periodicScheduling = false;
					triggerRequestQueued = false;

					// shut down the thread that handles the timeouts and pending triggers
					timer.cancel();

					// make sure that the actor does not linger
					if (jobStatusListener != null) {
						jobStatusListener.tell(PoisonPill.getInstance());
						jobStatusListener = null;
					}

					// clear and discard all pending checkpoints
					for (PendingCheckpoint pending : pendingCheckpoints.values()) {
						pending.discard(userClassLoader);
					}
					pendingCheckpoints.clear();

					if (shutdownStoreAndCounter) {
						completedCheckpointStore.shutdown();
						checkpointIdCounter.shutdown();
					} else {
						completedCheckpointStore.suspend();
						checkpointIdCounter.suspend();
					}

					onShutdown();
				}
			} finally {
				// Remove shutdown hook to prevent resource leaks, unless this is invoked by the
				// shutdown hook itself.
				if (shutdownHook != null && shutdownHook != Thread.currentThread()) {
					try {
						Runtime.getRuntime().removeShutdownHook(shutdownHook);
					}
					catch (IllegalStateException ignored) {
						// race, JVM is in shutdown already, we can safely ignore this
					}
					catch (Throwable t) {
						LOG.warn("Error unregistering checkpoint coordinator shutdown hook.", t);
					}
				}
			}
		}
	}

	public boolean isShutdown() {
		return shutdown;
	}

	// --------------------------------------------------------------------------------------------
	//  Handling checkpoints and messages
	// --------------------------------------------------------------------------------------------

	/**
	 * Triggers a new checkpoint and uses the given timestamp as the checkpoint
	 * timestamp.
	 *
	 * @param timestamp The timestamp for the checkpoint.
	 */
	public boolean triggerCheckpoint(long timestamp) throws Exception {
		return triggerCheckpoint(timestamp, -1);
	}

	/**
	 * Triggers a new checkpoint and uses the given timestamp as the checkpoint
	 * timestamp.
	 *
	 * @param timestamp The timestamp for the checkpoint.
	 * @param nextCheckpointId The checkpoint ID to use for this checkpoint or <code>-1</code> if
	 *                         the checkpoint ID counter should be queried.
	 */
	public boolean triggerCheckpoint(long timestamp, long nextCheckpointId) {
		// make some eager pre-checks
		synchronized (lock) {
			// abort if the coordinator has been shutdown in the meantime
			if (shutdown) {
				return false;
			}

			// sanity check: there should never be more than one trigger request queued
			if (triggerRequestQueued) {
				LOG.warn("Trying to trigger another checkpoint while one was queued already");
				return false;
			}

			//如果pendingCheckpoints大于最大并行调度的checkpoint，即当前的无法再并行的调度多一次的checkpoint，则设置triggerRequestQueued
			// if too many checkpoints are currently in progress, we need to mark that a request is queued
			//取消本次的触发checkpoint调度
			if (pendingCheckpoints.size() >= maxConcurrentCheckpointAttempts) {
				triggerRequestQueued = true;
				if (currentPeriodicTrigger != null) {
					currentPeriodicTrigger.cancel();
					currentPeriodicTrigger = null;
				}
				return false;
			}

			// make sure the minimum interval between checkpoints has passed
			final long earliestNext = lastCheckpointCompletionNanos + minPauseBetweenCheckpointsNanos;//保证最后一次checkpoint完成的时间加上暂时的时间间隔小于当前时间
			final long durationTillNextMillis = (earliestNext - System.nanoTime()) / 1_000_000;

			//如果不小于，说明最后一次完成的checkpoint时间没有经过暂停
			if (durationTillNextMillis > 0 && baseInterval != Long.MAX_VALUE) {
				if (currentPeriodicTrigger != null) {
					currentPeriodicTrigger.cancel();//取消这次的checkpoint调度，
				}
				currentPeriodicTrigger = new ScheduledTrigger();//重新生成一个checkpoint调度
				timer.scheduleAtFixedRate(currentPeriodicTrigger, durationTillNextMillis, baseInterval);//直到最后一次checkpoint完成的时间加上暂停时间追上当前时间，才开始调度
				return false;
			}
		}

		// first check if all tasks that we need to trigger are running.
		// if not, abort the checkpoint//保证我们需要触发的task都是运行状态
		ExecutionAttemptID[] triggerIDs = new ExecutionAttemptID[tasksToTrigger.length];
		for (int i = 0; i < tasksToTrigger.length; i++) {
			Execution ee = tasksToTrigger[i].getCurrentExecutionAttempt();
			if (ee != null && ee.getState() == ExecutionState.RUNNING) {
				triggerIDs[i] = ee.getAttemptId();
			} else {
				LOG.info("Checkpoint triggering task {} is not being executed at the moment. Aborting checkpoint.",
						tasksToTrigger[i].getSimpleName());
				return false;
			}
		}

		// next, check if all tasks that need to acknowledge the checkpoint are running.
		// if not, abort the checkpoint，保证需要ack的task都是活着的
		Map<ExecutionAttemptID, ExecutionVertex> ackTasks = new HashMap<>(tasksToWaitFor.length);

		for (ExecutionVertex ev : tasksToWaitFor) {
			Execution ee = ev.getCurrentExecutionAttempt();
			if (ee != null) {
				ackTasks.put(ee.getAttemptId(), ev);
			} else {
				LOG.info("Checkpoint acknowledging task {} is not being executed at the moment. Aborting checkpoint.",
						ev.getSimpleName());
				return false;
			}
		}

		// we will actually trigger this checkpoint!触发这次checkpoint，主要生成checkpoint id
		final long checkpointID;
		if (nextCheckpointId < 0) {
			try {
				// this must happen outside the locked scope, because it communicates
				// with external services (in HA mode) and may block for a while.
				checkpointID = checkpointIdCounter.getAndIncrement();//生成checkpoint ID
			}
			catch (Throwable t) {
				int numUnsuccessful = ++numUnsuccessfulCheckpointsTriggers;
				LOG.warn("Failed to trigger checkpoint (" + numUnsuccessful + " consecutive failed attempts so far)", t);
				return false;
			}
		}
		else {
			checkpointID = nextCheckpointId;
		}

		LOG.info("Triggering checkpoint " + checkpointID + " @ " + timestamp);
		//首先构造根据checkpointID及tiestamp构造PendingCheckpoint对象

		final PendingCheckpoint checkpoint = new PendingCheckpoint(
			job,
			checkpointID,
			timestamp,
			ackTasks,
			executor);

		// schedule the timer that will clean up the expired checkpoints
		TimerTask canceller = new TimerTask() {
			@Override
			public void run() {
				synchronized (lock) {
					// only do the work if the checkpoint is not discarded anyways
					// note that checkpoint completion discards the pending checkpoint object
					if (!checkpoint.isDiscarded()) {
						LOG.info("Checkpoint " + checkpointID + " expired before completing.");

						checkpoint.discard(userClassLoader);
						pendingCheckpoints.remove(checkpointID);
						rememberRecentCheckpointId(checkpointID);

						onCancelCheckpoint(checkpointID);

						triggerQueuedRequests();
					}
				}
			}
		};

		try {
			// re-acquire the lock
			synchronized (lock) {
				// since we released the lock in the meantime, we need to re-check
				// that the conditions still hold. this is clumsy, but it allows us to
				// release the lock in the meantime while calls to external services are
				// blocking progress, and still gives us early checks that skip work
				// if no checkpoint can happen anyways
				if (shutdown) {
					return false;
				}
				else if (triggerRequestQueued) {
					LOG.warn("Trying to trigger another checkpoint while one was queued already");
					return false;
				}
				else if (pendingCheckpoints.size() >= maxConcurrentCheckpointAttempts) {
					triggerRequestQueued = true;
					if (currentPeriodicTrigger != null) {
						currentPeriodicTrigger.cancel();
						currentPeriodicTrigger = null;
					}
					return false;
				}

				// make sure the minimum interval between checkpoints has passed
				final long earliestNext = lastCheckpointCompletionNanos + minPauseBetweenCheckpointsNanos;
				final long durationTillNextMillis = (earliestNext - System.nanoTime()) / 1_000_000;

				if (durationTillNextMillis > 0 && baseInterval != Long.MAX_VALUE) {
					if (currentPeriodicTrigger != null) {
						currentPeriodicTrigger.cancel();
					}
					currentPeriodicTrigger = new ScheduledTrigger();
					timer.scheduleAtFixedRate(currentPeriodicTrigger, durationTillNextMillis, baseInterval);
					return false;
				}

				pendingCheckpoints.put(checkpointID, checkpoint);//将pending checkpoint 加入pendingCheckpoints
				timer.schedule(canceller, checkpointTimeout);//启动超时回调
			}
			// end of lock scope
			//发送TriggerCheckpoint消息给task触发checkpoint

			// send the messages to the tasks that trigger their checkpoint
			for (int i = 0; i < tasksToTrigger.length; i++) {
				ExecutionAttemptID id = triggerIDs[i];
				TriggerCheckpoint message = new TriggerCheckpoint(job, id, checkpointID, timestamp);
				tasksToTrigger[i].sendMessageToCurrentExecution(message, id);
			}

			numUnsuccessfulCheckpointsTriggers = 0;
			return true;
		}
		catch (Throwable t) {
			// guard the map against concurrent modifications
			synchronized (lock) {
				pendingCheckpoints.remove(checkpointID);
			}

			int numUnsuccessful = ++numUnsuccessfulCheckpointsTriggers;
			LOG.warn("Failed to trigger checkpoint (" + numUnsuccessful + " consecutive failed attempts so far)", t);
			if (!checkpoint.isDiscarded()) {
				checkpoint.discard(userClassLoader);
			}
			return false;
		}
	}

	/**
	 * Receives a {@link DeclineCheckpoint} message and returns whether the
	 * message was associated with a pending checkpoint.
	 *
	 * @param message Checkpoint decline from the task manager
	 *
	 * @return Flag indicating whether the declined checkpoint was associated
	 * with a pending checkpoint.
	 */
	public boolean receiveDeclineMessage(DeclineCheckpoint message) {
		if (shutdown || message == null) {
			return false;
		}
		if (!job.equals(message.getJob())) {
			LOG.error("Received DeclineCheckpoint message for wrong job: {}", message);
			return false;
		}

		final long checkpointId = message.getCheckpointId();//获取checkpoint id
		final String reason = (message.getReason() != null ? message.getReason().getMessage() : "");

		PendingCheckpoint checkpoint;

		// Flag indicating whether the ack message was for a known pending
		// checkpoint.
		boolean isPendingCheckpoint;

		synchronized (lock) {
			// we need to check inside the lock for being shutdown as well, otherwise we
			// get races and invalid error log messages
			if (shutdown) {
				return false;
			}

			checkpoint = pendingCheckpoints.get(checkpointId);
			//如果从pendingCheckpoints能够获取该checkpointID对应的checkpoint
			//证明本次消息就是对应pendingCheckpoint的，设置isPendingCheckpoint为true

			if (checkpoint != null && !checkpoint.isDiscarded()) {
				isPendingCheckpoint = true;
				//打印该消息产生的原因

				LOG.info("Discarding checkpoint {} because of checkpoint decline from task {} : {}",
						checkpointId, message.getTaskExecutionId(), reason);

				pendingCheckpoints.remove(checkpointId);//pendingCheckpoints中移除本次checkpoint
				checkpoint.discard(userClassLoader);//丢弃该checkpoint关联的resource
				rememberRecentCheckpointId(checkpointId);//recentPendingCheckpoints队列中设置最新的checkpointID为当前checkpoint ID

				onCancelCheckpoint(checkpointId);//通知savepoint coordinator移除本次checkpoint

				boolean haveMoreRecentPending = false;
				//如果pendingCheckpoints中存在checkpoint id 大于本次的checkpoint ID，则表明仍然有更多处于pending状态的checkpoint
				for (PendingCheckpoint p : pendingCheckpoints.values()) {
					if (!p.isDiscarded() && p.getCheckpointId() >= checkpoint.getCheckpointId()) {
						haveMoreRecentPending = true; //pendingCheckpoints存在大于本次checkpointID的checkpoint
						break;
					}
				}

				if (!haveMoreRecentPending) {
					triggerQueuedRequests();//如果没有haveMoreRecentPending，则立即触发下一个checkpoint
				}
			} else if (checkpoint != null) {//如果是未完成的检查点，并且检查点已经被discarded
				// this should not happen
				throw new IllegalStateException(
					"Received message for discarded but non-removed checkpoint " + checkpointId);
			} else {
				//如果在最近未完成的检查点列表中找到，则有可能表示消息来迟了，将isPendingCheckpoint置为true，否则将isPendingCheckpoint置为false
				// message is for an unknown checkpoint, or comes too late (checkpoint disposed)
				if (recentPendingCheckpoints.contains(checkpointId)) {
					isPendingCheckpoint = true;
					LOG.info("Received another decline checkpoint message for now expired checkpoint attempt " + checkpointId);
				} else {
					isPendingCheckpoint = false;
				}
			}
		}

		return isPendingCheckpoint;
	}

	/**
	 * Receives an AcknowledgeCheckpoint message and returns whether the
	 * message was associated with a pending checkpoint.
	 *
	 * @param message Checkpoint ack from the task manager
	 *
	 * @return Flag indicating whether the ack'd checkpoint was associated
	 * with a pending checkpoint.
	 *
	 * @throws Exception If the checkpoint cannot be added to the completed checkpoint store.
	 */
	public boolean receiveAcknowledgeMessage(AcknowledgeCheckpoint message) throws CheckpointException {
		if (shutdown || message == null) {
			return false;
		}

		if (!job.equals(message.getJob())) {
			LOG.error("Received wrong AcknowledgeCheckpoint message for job {}: {}", job, message);
			return false;
		}

		final long checkpointId = message.getCheckpointId();//获取checkpoint ID

		synchronized (lock) {
			// we need to check inside the lock for being shutdown as well, otherwise we
			// get races and invalid error log messages
			if (shutdown) {
				return false;
			}

			final PendingCheckpoint checkpoint = pendingCheckpoints.get(checkpointId);//确认是否是pending的checkpoint，所有的checkpoint都会首先加入到pendingCheckpoints

			//是pending checkpoint且没有被discard

			if (checkpoint != null && !checkpoint.isDiscarded()) {

				switch (checkpoint.acknowledgeTask(message.getTaskExecutionId(), message.getState(), message.getStateSize(), null)) {
					case SUCCESS:
						if (checkpoint.isFullyAcknowledged()) {//checkpoint被task acked
							completePendingCheckpoint(checkpoint);
							
						}
						break;
					case DUPLICATE:
						LOG.debug("Received a duplicate acknowledge message for checkpoint {}, task {}, job {}.",
							message.getCheckpointId(), message.getTaskExecutionId(), message.getJob());
						break;
					case UNKNOWN:
						LOG.warn("Could not acknowledge the checkpoint {} for task {} of job {}, " +
								"because the task's execution attempt id was unknown. Discarding " +
								"the state handle to avoid lingering state.", message.getCheckpointId(),
							message.getTaskExecutionId(), message.getJob());

						discardState(message.getJob(), message.getTaskExecutionId(), checkpointId, message.getState());
						break;
					case DISCARDED:
						LOG.warn("Could not acknowledge the checkpoint {} for task {} of job {}, " +
							"because the pending checkpoint had been discarded. Discarding the " +
								"state handle tp avoid lingering state.",
							message.getCheckpointId(), message.getTaskExecutionId(), message.getJob());

						discardState(message.getJob(), message.getTaskExecutionId(), checkpointId, message.getState());
				}

				return true;
			}
			else if (checkpoint != null) {
				// this should not happen
				throw new IllegalStateException(
						"Received message for discarded but non-removed checkpoint " + checkpointId);
			}
			else {
				boolean wasPendingCheckpoint;

				// message is for an unknown checkpoint, or comes too late (checkpoint disposed)
				if (recentPendingCheckpoints.contains(checkpointId)) {
					wasPendingCheckpoint = true;
					LOG.warn("Received late message for now expired checkpoint attempt {} from " +
						"task {} and job {}.", checkpointId, message.getTaskExecutionId(), message.getJob());

					// try to discard the state so that we don't have lingering state lying around
					discardState(message.getJob(), message.getTaskExecutionId(), checkpointId, message.getState());
				}
				else {
					LOG.debug("Received message for an unknown checkpoint {} from task {} and job" +
						" {}.", checkpointId, message.getTaskExecutionId(), message.getState());
					wasPendingCheckpoint = false;
				}

				return wasPendingCheckpoint;
			}
		}
	}

	/**
	 * Try to complete the given pending checkpoint.
	 * 完成本次的pending checkpoint
	 *
	 * Important: This method should only be called in the checkpoint lock scope.
	 *
	 * @param pendingCheckpoint to complete
	 * @throws CheckpointException if the completion failed
	 */
	private void completePendingCheckpoint(PendingCheckpoint pendingCheckpoint) throws CheckpointException {
		// we have to be called in the checkpoint lock scope
		assert(Thread.holdsLock(lock));

		final long checkpointId = pendingCheckpoint.getCheckpointId();
		CompletedCheckpoint completedCheckpoint = null;

		try {
			completedCheckpoint = pendingCheckpoint.finalizeCheckpoint();			

			completedCheckpointStore.addCheckpoint(completedCheckpoint);//塞进completedCheckpointStore

			rememberRecentCheckpointId(checkpointId);//标记此次checkpoint Id为最近的checkpoint
			dropSubsumedCheckpoints(completedCheckpoint.getTimestamp());//回收这个checkpoint以前的checkpoint 信息，

			onFullyAcknowledgedCheckpoint(completedCheckpoint);//savepoint remove(checkpoint.getCheckpointID())
		} catch (Exception exception) {
			// abort the current pending checkpoint if it has not been discarded yet
			if (!pendingCheckpoint.isDiscarded()) {
				pendingCheckpoint.discard(userClassLoader);
			}

			if (completedCheckpoint != null) {
				// we failed to store the completed checkpoint. Let's clean up
				final CompletedCheckpoint cc = completedCheckpoint;

				executor.execute(new Runnable() {
					@Override
					public void run() {
						try {
							cc.discard(userClassLoader);
						} catch (Exception nestedException) {
							LOG.warn("Could not properly discard completed checkpoint {}.", cc.getCheckpointID(), nestedException);
						}
					}
				});
			}

			throw new CheckpointException("Could not complete the pending checkpoint " + checkpointId + '.', exception);
		} finally {
			pendingCheckpoints.remove(checkpointId);//最后从pendingCheckpoints移除这个checkpoint

			triggerQueuedRequests();//触发下一个定时checkpoint
		}
		
		lastCheckpointCompletionNanos = System.nanoTime();//最后一次checkpoint完成的时间为当前时间

		LOG.info("Completed checkpoint {} (in {} ms).", checkpointId, completedCheckpoint.getDuration());

		if (LOG.isDebugEnabled()) {
			StringBuilder builder = new StringBuilder();
			builder.append("Checkpoint state: ");
			for (TaskState state : completedCheckpoint.getTaskStates().values()) {
				builder.append(state);
				builder.append(", ");
			}
			// Remove last two chars ", "
			builder.delete(builder.length() - 2, builder.length());

			LOG.debug(builder.toString());
		}

		final long timestamp = completedCheckpoint.getTimestamp();

		for (ExecutionVertex ev : tasksToCommitTo) {
			Execution ee = ev.getCurrentExecutionAttempt();//如果tasksToCommitTo里面有Current ExecutionAttempt()，即当前task的checkpoint已经ack过上尚未commit
			if (ee != null) {
					ExecutionAttemptID attemptId = ee.getAttemptId();
				    //以checkpointID，ExecutionAttemptID，timestamp构造NotifyCheckpointComplete消息由ev路由给TaskManager
					NotifyCheckpointComplete notifyMessage = new NotifyCheckpointComplete(job, attemptId, checkpointId, timestamp);
					ev.sendMessageToCurrentExecution(notifyMessage, ee.getAttemptId());
			}
		}

		statsTracker.onCompletedCheckpoint(completedCheckpoint);//statsTracker标记此次checkpoint已经complete
	}

	private void rememberRecentCheckpointId(long id) {
		if (recentPendingCheckpoints.size() >= NUM_GHOST_CHECKPOINT_IDS) {
			recentPendingCheckpoints.removeFirst();
		}
		recentPendingCheckpoints.addLast(id);
	}

	private void dropSubsumedCheckpoints(long timestamp) {
		Iterator<Map.Entry<Long, PendingCheckpoint>> entries = pendingCheckpoints.entrySet().iterator();
		while (entries.hasNext()) {
			PendingCheckpoint p = entries.next().getValue();
			if (p.getCheckpointTimestamp() < timestamp) {
				rememberRecentCheckpointId(p.getCheckpointId());//这里是不是有问题，最新的一次不是应该还是timestamp对应的？？

				p.discard(userClassLoader);

				onCancelCheckpoint(p.getCheckpointId());

				entries.remove();
			}
		}
	}

	/**
	 * Triggers the queued request, if there is one.
	 *
	 * <p>NOTE: The caller of this method must hold the lock when invoking the method!
	 */
	private void triggerQueuedRequests() {
		assert Thread.holdsLock(lock);

		if (triggerRequestQueued) {
			triggerRequestQueued = false;

			// trigger the checkpoint from the trigger timer, to finish the work of this thread before
			// starting with the next checkpoint
			ScheduledTrigger trigger = new ScheduledTrigger();
			if (periodicScheduling) {
				if (currentPeriodicTrigger != null) {
					currentPeriodicTrigger.cancel();
				}
				currentPeriodicTrigger = trigger;//立即触发下一个定时checkpoint，主要是启动ScheduledTriggerrun方法中triggerCheckpoint(System.currentTimeMillis())
				timer.scheduleAtFixedRate(trigger, 0L, baseInterval);
			}
			else {
				timer.schedule(trigger, 0L);
			}
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Checkpoint State Restoring
	// --------------------------------------------------------------------------------------------

	public boolean restoreLatestCheckpointedState(
			Map<JobVertexID, ExecutionJobVertex> tasks,
			boolean errorIfNoCheckpoint,
			boolean allOrNothingState) throws Exception {

		synchronized (lock) {
			if (shutdown) {
				throw new IllegalStateException("CheckpointCoordinator is shut down");
			}

			// Recover the checkpoints
			completedCheckpointStore.recover();

			// restore from the latest checkpoint
			CompletedCheckpoint latest = completedCheckpointStore.getLatestCheckpoint();

			if (latest == null) {
				if (errorIfNoCheckpoint) {
					throw new IllegalStateException("No completed checkpoint available");
				} else {
					return false;
				}
			}

			for (Map.Entry<JobVertexID, TaskState> taskGroupStateEntry: latest.getTaskStates().entrySet()) {
				TaskState taskState = taskGroupStateEntry.getValue();
				ExecutionJobVertex executionJobVertex = tasks.get(taskGroupStateEntry.getKey());

				if (executionJobVertex != null) {
					// check that we only restore the state if the parallelism has not been changed
					if (taskState.getParallelism() != executionJobVertex.getParallelism()) {
						throw new RuntimeException("Cannot restore the latest checkpoint because " +
							"the parallelism changed. The operator" + executionJobVertex.getJobVertexId() +
							" has parallelism " + executionJobVertex.getParallelism() + " whereas the corresponding" +
							"state object has a parallelism of " + taskState.getParallelism());
					}

					int counter = 0;

					List<Set<Integer>> keyGroupPartitions = createKeyGroupPartitions(numberKeyGroups, executionJobVertex.getParallelism());

					for (int i = 0; i < executionJobVertex.getParallelism(); i++) {
						SubtaskState subtaskState = taskState.getState(i);
						SerializedValue<StateHandle<?>> state = null;

						if (subtaskState != null) {
							// count the number of executions for which we set a state
							counter++;
							state = subtaskState.getState();
						}

						Map<Integer, SerializedValue<StateHandle<?>>> kvStateForTaskMap = taskState.getUnwrappedKvStates(keyGroupPartitions.get(i));

						Execution currentExecutionAttempt = executionJobVertex.getTaskVertices()[i].getCurrentExecutionAttempt();
						currentExecutionAttempt.setInitialState(state, kvStateForTaskMap);
					}

					if (allOrNothingState && counter > 0 && counter < executionJobVertex.getParallelism()) {
						throw new IllegalStateException("The checkpoint contained state only for " +
							"a subset of tasks for vertex " + executionJobVertex);
					}
				} else {
					throw new IllegalStateException("There is no execution job vertex for the job" +
						" vertex ID " + taskGroupStateEntry.getKey());
				}
			}

			return true;
		}
	}

	/**
	 * Groups the available set of key groups into key group partitions. A key group partition is
	 * the set of key groups which is assigned to the same task. Each set of the returned list
	 * constitutes a key group partition.
	 *
	 * @param numberKeyGroups Number of available key groups (indexed from 0 to numberKeyGroups - 1)
	 * @param parallelism Parallelism to generate the key group partitioning for
	 * @return List of key group partitions
	 */
	protected List<Set<Integer>> createKeyGroupPartitions(int numberKeyGroups, int parallelism) {
		ArrayList<Set<Integer>> result = new ArrayList<>(parallelism);

		for (int p = 0; p < parallelism; p++) {
			HashSet<Integer> keyGroupPartition = new HashSet<>();

			for (int k = p; k < numberKeyGroups; k += parallelism) {
				keyGroupPartition.add(k);
			}

			result.add(keyGroupPartition);
		}

		return result;
	}

	// --------------------------------------------------------------------------------------------
	//  Accessors
	// --------------------------------------------------------------------------------------------

	public int getNumberOfPendingCheckpoints() {
		return this.pendingCheckpoints.size();
	}

	public int getNumberOfRetainedSuccessfulCheckpoints() {
		synchronized (lock) {
			return completedCheckpointStore.getNumberOfRetainedCheckpoints();
		}
	}

	public Map<Long, PendingCheckpoint> getPendingCheckpoints() {
		synchronized (lock) {
			return new HashMap<Long, PendingCheckpoint>(this.pendingCheckpoints);
		}
	}

	public List<CompletedCheckpoint> getSuccessfulCheckpoints() throws Exception {
		synchronized (lock) {
			return completedCheckpointStore.getAllCheckpoints();
		}
	}

	protected long getAndIncrementCheckpointId() {
		try {
			// this must happen outside the locked scope, because it communicates
			// with external services (in HA mode) and may block for a while.
			return checkpointIdCounter.getAndIncrement();
		}
		catch (Throwable t) {
			int numUnsuccessful = ++numUnsuccessfulCheckpointsTriggers;
			LOG.warn("Failed to trigger checkpoint (" + numUnsuccessful + " consecutive failed attempts so far)", t);
			return -1;
		}
	}

	protected ActorGateway getJobStatusListener() {
		return jobStatusListener;
	}

	protected void setJobStatusListener(ActorGateway jobStatusListener) {
		this.jobStatusListener = jobStatusListener;
	}

	// --------------------------------------------------------------------------------------------
	//  Periodic scheduling of checkpoints
	// --------------------------------------------------------------------------------------------
 //调度周期的checkpoint
	public void startCheckpointScheduler() throws Exception {
		synchronized (lock) {
			if (shutdown) {
				throw new IllegalArgumentException("Checkpoint coordinator is shut down");
			}

			// make sure all prior timers are cancelled
			stopCheckpointScheduler();//？确保之前的timer都是停止的

			try {
				// Multiple start calls are OK
				checkpointIdCounter.start();
			} catch (Exception e) {
				String msg = "Failed to start checkpoint ID counter: " + e.getMessage();
				throw new RuntimeException(msg, e);
			}

			periodicScheduling = true;
			currentPeriodicTrigger = new ScheduledTrigger();
			timer.scheduleAtFixedRate(currentPeriodicTrigger, baseInterval, baseInterval);//启动ScheduledTrigger线程触发checkpoint
		}
	}

	public void stopCheckpointScheduler() throws Exception {
		synchronized (lock) {
			triggerRequestQueued = false;
			periodicScheduling = false;

			if (currentPeriodicTrigger != null) {
				currentPeriodicTrigger.cancel();
				currentPeriodicTrigger = null;
			}

			for (PendingCheckpoint p : pendingCheckpoints.values()) {
				p.discard(userClassLoader);
			}
			pendingCheckpoints.clear();

			numUnsuccessfulCheckpointsTriggers = 0;
		}
	}

	// ------------------------------------------------------------------------
	//  job status listener that schedules / cancels periodic checkpoints
	// ------------------------------------------------------------------------

	public ActorGateway createActivatorDeactivator(ActorSystem actorSystem, UUID leaderSessionID) {
		synchronized (lock) {
			if (shutdown) {
				throw new IllegalArgumentException("Checkpoint coordinator is shut down");
			}

			if (jobStatusListener == null) {
				//这里构造CheckpointCoordinatorDeActivator actorRef,通过向该actorRef发送JobStatusChanged消息来启停定时cp调度器
				Props props = Props.create(CheckpointCoordinatorDeActivator.class, this, leaderSessionID);

				// wrap the ActorRef in a AkkaActorGateway to support message decoration
				jobStatusListener = new AkkaActorGateway(actorSystem.actorOf(props), leaderSessionID);
			}

			return jobStatusListener;
		}
	}

	// ------------------------------------------------------------------------

	private class ScheduledTrigger extends TimerTask {

		@Override
		public void run() {
			try {
				triggerCheckpoint(System.currentTimeMillis());//以当前时间作为时间戳触发checkpoint
			}
			catch (Exception e) {
				LOG.error("Exception while triggering checkpoint", e);
			}
		}
	}

	private void discardState(
			final JobID jobId,
			final ExecutionAttemptID executionAttemptID,
			final long checkpointId,
			final SerializedValue<StateHandle<?>> stateObject) {
		if (stateObject != null) {
			executor.execute(new Runnable() {
				@Override
				public void run() {
					try {
						stateObject.deserializeValue(userClassLoader).discardState();
					} catch (Exception e) {
						LOG.warn("Could not properly discard state object for checkpoint {} " +
							"belonging to task {} of job {}.", checkpointId,
							executionAttemptID, jobId, e);
					}
				}
			});
		}
	}
}
