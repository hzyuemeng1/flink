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

package org.apache.flink.streaming.api.graph;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import org.apache.commons.lang3.StringUtils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.operators.util.UserCodeObjectWrapper;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.InputFormatVertex;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.JobSnapshottingSettings;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.transformations.StreamTransformation;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RescalePartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.tasks.StreamIterationHead;
import org.apache.flink.streaming.runtime.tasks.StreamIterationTail;
import org.apache.flink.util.InstantiationUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;

import static org.apache.flink.util.StringUtils.byteToHexString;

@Internal
public class StreamingJobGraphGenerator {

	private static final Logger LOG = LoggerFactory.getLogger(StreamingJobGraphGenerator.class);

	/**
	 * Restart delay used for the FixedDelayRestartStrategy in case checkpointing was enabled but
	 * no restart strategy has been specified.
	 */
	private static final long DEFAULT_RESTART_DELAY = 10000L;

	private StreamGraph streamGraph;
	// id -> JobVertex
	private Map<Integer, JobVertex> jobVertices;
	private JobGraph jobGraph;

	// 已经构建的JobVertex的id集合
	private Collection<Integer> builtVertices;

	// 物理边集合（排除了chain内部的边）, 按创建顺序排序
	private List<StreamEdge> physicalEdgesInOrder;

	// 保存chain信息，部署时用来构建 OperatorChain，startNodeId -> (currentNodeId -> StreamConfig)
	private Map<Integer, Map<Integer, StreamConfig>> chainedConfigs;
	// 所有节点的配置信息，id -> StreamConfig
	private Map<Integer, StreamConfig> vertexConfigs;
	// 保存每个节点的名字，id -> chainedName
	private Map<Integer, String> chainedNames;

	// 构造函数，入参只有 StreamGraph
	public StreamingJobGraphGenerator(StreamGraph streamGraph) {
		this.streamGraph = streamGraph;
	}

	private void init() {
		this.jobVertices = new HashMap<>();
		this.builtVertices = new HashSet<>();
		this.chainedConfigs = new HashMap<>();
		this.vertexConfigs = new HashMap<>();
		this.chainedNames = new HashMap<>();
		this.physicalEdgesInOrder = new ArrayList<>();
	}

	public JobGraph createJobGraph() {
		jobGraph = new JobGraph(streamGraph.getJobName());
		// streaming 模式下，调度模式是所有节点（vertices）一起启动
		// make sure that all vertices start immediately
		jobGraph.setScheduleMode(ScheduleMode.ALL);
       // 初始化成员变量
		init();

		// 广度优先遍历 StreamGraph 并且为每个SteamNode生成hash id，
		// 保证如果提交的拓扑没有改变，则每次生成的hash都是一样的
		// Generate deterministic hashes for the nodes in order to identify them across
		// submission iff they didn't change.       key是nodeID，值是hash
		Map<Integer, byte[]> hashes = traverseStreamGraphAndGenerateHashes();

		// 最重要的函数，生成JobVertex，JobEdge等，并尽可能地将多个节点chain在一起
		setChaining(hashes);

		// 将每个JobVertex的入边集合也序列化到该JobVertex的StreamConfig中
		// (出边集合已经在setChaining的时候写入了)
		setPhysicalEdges();

		// 根据group name，为每个 JobVertex 指定所属的 SlotSharingGroup
		// 以及针对 Iteration的头尾设置  CoLocationGroup
		setSlotSharing();
		// 配置checkpoint
		configureCheckpointing();
		// 配置重启策略（不重启，还是固定延迟重启）
		configureRestartStrategy();

		try {
			// 将 StreamGraph 的 ExecutionConfig 序列化到 JobGraph 的配置中
			InstantiationUtil.writeObjectToConfig(this.streamGraph.getExecutionConfig(), this.jobGraph.getJobConfiguration(), ExecutionConfig.CONFIG_KEY);
		} catch (IOException e) {
			throw new RuntimeException("Config object could not be written to Job Configuration: ", e);
		}
		
		return jobGraph;
	}

	private void setPhysicalEdges() {
		Map<Integer, List<StreamEdge>> physicalInEdgesInOrder = new HashMap<Integer, List<StreamEdge>>();

		for (StreamEdge edge : physicalEdgesInOrder) {
			int target = edge.getTargetId();

			List<StreamEdge> inEdges = physicalInEdgesInOrder.get(target);

			// create if not set
			if (inEdges == null) {
				inEdges = new ArrayList<>();
				physicalInEdgesInOrder.put(target, inEdges);
			}

			inEdges.add(edge);
		}

		for (Map.Entry<Integer, List<StreamEdge>> inEdges : physicalInEdgesInOrder.entrySet()) {
			int vertex = inEdges.getKey();
			List<StreamEdge> edgeList = inEdges.getValue();

			vertexConfigs.get(vertex).setInPhysicalEdges(edgeList);
		}
	}

	/**
	 * Sets up task chains from the source {@link StreamNode} instances.
	 *
	 * <p>This will recursively create all {@link JobVertex} instances.
	 */
	private void setChaining(Map<Integer, byte[]> hashes) {
		//先从源节点开始
		for (Integer sourceNodeId : streamGraph.getSourceIDs()) {
			createChain(sourceNodeId, sourceNodeId, hashes);
		}
	}
		// 构建node chains，返回当前节点的物理出边
		// startNodeId != currentNodeId 时,说明currentNode是chain中的子节点
	private List<StreamEdge> createChain( //刚开始两个参数都为源节点
			Integer startNodeId,
			Integer currentNodeId,
			Map<Integer, byte[]> hashes) {

		if (!builtVertices.contains(startNodeId)) {
			// 过渡用的出边集合, 用来生成最终的 JobEdge, 注意不包括 chain 内部的边。说明白点就是不能链上的操作
			List<StreamEdge> transitiveOutEdges = new ArrayList<StreamEdge>();

			List<StreamEdge> chainableOutputs = new ArrayList<StreamEdge>();
			List<StreamEdge> nonChainableOutputs = new ArrayList<StreamEdge>();

			// 将当前节点的出边分成 chainable 和 nonChainable 两类
			for (StreamEdge outEdge : streamGraph.getStreamNode(currentNodeId).getOutEdges()) {
				if (isChainable(outEdge)) {
					chainableOutputs.add(outEdge);
				} else {
					nonChainableOutputs.add(outEdge);
				}
			}

			//==> 递归调用
			/**
			 * 这个整个方法，返回的就是transitiveOutEdges，这个transitiveOutEdges却是
			 * 递归调用这个方法拿到的，
			 * 这个方法是拿到第二个参数的出边。。
			 *
			 * 注意不仅仅递归，还把不能递归的加上
			 *
			 *
			 */
			for (StreamEdge chainable : chainableOutputs) {
				transitiveOutEdges.addAll(createChain(startNodeId, chainable.getTargetId(), hashes));
			}
			for (StreamEdge nonChainable : nonChainableOutputs) {
				transitiveOutEdges.add(nonChainable);
				createChain(nonChainable.getTargetId(), nonChainable.getTargetId(), hashes);
			}

			// 生成当前节点的显示名，如："Keyed Aggregation -> Sink: Unnamed"
			chainedNames.put(currentNodeId, createChainedName(currentNodeId, chainableOutputs));

			// 如果当前节点是起始节点, 则直接创建 JobVertex 并返回 StreamConfig, 否则先创建一个空的 StreamConfig
			// createJobVertex 函数就是根据 StreamNode 创建对应的 JobVertex, 并返回了空的 StreamConfig
			/**
			 * 什么时候会相等呢，看233行：
			 *  createChain(nonChainable.getTargetId(), nonChainable.getTargetId(), hashes);
			 *
			 *
			 *
			 *
			 *  注意这里是返回了JobVertex的jobVertex.getConfiguration()，StreamConfig持有这个引用，s
			 *  所以之后对这个StreamConfig添加东西，都添加到了JobVertex上了
			 *
			 */
			StreamConfig config = currentNodeId.equals(startNodeId)
				? createJobVertex(startNodeId, hashes)
				: new StreamConfig(new Configuration());

			// 设置 JobVertex 的 StreamConfig, 基本上是序列化 StreamNode 中的配置到 StreamConfig 中.
			// 其中包括 序列化器, StreamOperator, Checkpoint，输出边（包括了） 等相关配置
			setVertexConfig(currentNodeId, config, chainableOutputs, nonChainableOutputs);

			if (currentNodeId.equals(startNodeId)) {
				// 如果是chain的起始节点。（不是chain中的节点，也会被标记成 chain start）
				config.setChainStart();
				// 我们也会把物理出边写入配置, 部署时会用到
				config.setOutEdgesInOrder(transitiveOutEdges);
				//
				config.setOutEdges(streamGraph.getStreamNode(currentNodeId).getOutEdges());

				// 将当前节点(headOfChain)与所有出边相连
				for (StreamEdge edge : transitiveOutEdges) {
					// 通过StreamEdge构建出JobEdge，创建IntermediateDataSet，用来将JobVertex和JobEdge相连
					connect(startNodeId, edge);
				}
				/**
				 * 由于之前调用了递归，获得了物理出边，所以中间节点一定是被存了的
				 *
				 */
				// 将chain中所有子节点的StreamConfig写入到 headOfChain 节点的 CHAINED_TASK_CONFIG 配置中
				config.setTransitiveChainedTaskConfigs(chainedConfigs.get(startNodeId));

			} else {
				// 如果是 chain 中的子节点

				Map<Integer, StreamConfig> chainedConfs = chainedConfigs.get(startNodeId);

				if (chainedConfs == null) {
					chainedConfigs.put(startNodeId, new HashMap<Integer, StreamConfig>());
				}
				/**
				 *
				 * 非常注意，这个全局变量的key 存的是job顶点的ID，
				 * 值是这个顶点的中间节点配置
				 *
				 *
				 *
				 *
				 */
				// 将当前节点的StreamConfig添加到该chain的config集合中
				chainedConfigs.get(startNodeId).put(currentNodeId, config);
			}

			// 返回连往chain外部的出边集合
			return transitiveOutEdges;

		} else {
			return new ArrayList<>();
		}
	}

	private String createChainedName(Integer vertexID, List<StreamEdge> chainedOutputs) {
		String operatorName = streamGraph.getStreamNode(vertexID).getOperatorName();
		if (chainedOutputs.size() > 1) {
			List<String> outputChainedNames = new ArrayList<>();
			for (StreamEdge chainable : chainedOutputs) {
				outputChainedNames.add(chainedNames.get(chainable.getTargetId()));
			}
			return operatorName + " -> (" + StringUtils.join(outputChainedNames, ", ") + ")";
		} else if (chainedOutputs.size() == 1) {
			return operatorName + " -> " + chainedNames.get(chainedOutputs.get(0).getTargetId());
		} else {
			return operatorName;
		}
	}

	private StreamConfig createJobVertex(
			Integer streamNodeId,
			Map<Integer, byte[]> hashes) {

		JobVertex jobVertex;
		StreamNode streamNode = streamGraph.getStreamNode(streamNodeId);

		byte[] hash = hashes.get(streamNodeId);

		if (hash == null) {
			throw new IllegalStateException("Cannot find node hash. " +
					"Did you generate them before calling this method?");
		}

		JobVertexID jobVertexId = new JobVertexID(hash);

		if (streamNode.getInputFormat() != null) {
			jobVertex = new InputFormatVertex(
					chainedNames.get(streamNodeId),
					jobVertexId);
			TaskConfig taskConfig = new TaskConfig(jobVertex.getConfiguration());
			taskConfig.setStubWrapper(new UserCodeObjectWrapper<Object>(streamNode.getInputFormat()));
		} else {
			jobVertex = new JobVertex(
					chainedNames.get(streamNodeId),
					jobVertexId);
		}
        //设置运行的类
		jobVertex.setInvokableClass(streamNode.getJobVertexClass());

		int parallelism = streamNode.getParallelism();

		if (parallelism > 0) {
			jobVertex.setParallelism(parallelism);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("Parallelism set: {} for {}", parallelism, streamNodeId);
		}

		jobVertices.put(streamNodeId, jobVertex);
		builtVertices.add(streamNodeId);
		jobGraph.addVertex(jobVertex);

		return new StreamConfig(jobVertex.getConfiguration());
	}

	@SuppressWarnings("unchecked")
	private void setVertexConfig(Integer vertexID, StreamConfig config,
			List<StreamEdge> chainableOutputs, List<StreamEdge> nonChainableOutputs) {

		StreamNode vertex = streamGraph.getStreamNode(vertexID);

		config.setVertexID(vertexID);
		config.setBufferTimeout(vertex.getBufferTimeout());

		config.setTypeSerializerIn1(vertex.getTypeSerializerIn1());
		config.setTypeSerializerIn2(vertex.getTypeSerializerIn2());
		config.setTypeSerializerOut(vertex.getTypeSerializerOut());

		config.setStreamOperator(vertex.getOperator());
		config.setOutputSelectors(vertex.getOutputSelectors());

		config.setNumberOfOutputs(nonChainableOutputs.size());
		config.setNonChainedOutputs(nonChainableOutputs);

		//单独放进
		config.setChainedOutputs(chainableOutputs);

		config.setTimeCharacteristic(streamGraph.getEnvironment().getStreamTimeCharacteristic());
		
		final CheckpointConfig ceckpointCfg = streamGraph.getCheckpointConfig();
		
		config.setStateBackend(streamGraph.getStateBackend());
		config.setCheckpointingEnabled(ceckpointCfg.isCheckpointingEnabled());
		if (ceckpointCfg.isCheckpointingEnabled()) {
			config.setCheckpointMode(ceckpointCfg.getCheckpointingMode());
		}
		else {
			// the "at-least-once" input handler is slightly cheaper (in the absence of checkpoints),
			// so we use that one if checkpointing is not enabled
			config.setCheckpointMode(CheckpointingMode.AT_LEAST_ONCE);
		}
		config.setStatePartitioner(0, vertex.getStatePartitioner1());
		config.setStatePartitioner(1, vertex.getStatePartitioner2());
		config.setStateKeySerializer(vertex.getStateKeySerializer());
		
		Class<? extends AbstractInvokable> vertexClass = vertex.getJobVertexClass();

		if (vertexClass.equals(StreamIterationHead.class)
				|| vertexClass.equals(StreamIterationTail.class)) {
			config.setIterationId(streamGraph.getBrokerID(vertexID));
			config.setIterationWaitTime(streamGraph.getLoopTimeout(vertexID));
		}

		List<StreamEdge> allOutputs = new ArrayList<StreamEdge>(chainableOutputs);
		allOutputs.addAll(nonChainableOutputs);

		vertexConfigs.put(vertexID, config);
	}

	private void connect(Integer headOfChain, StreamEdge edge) {

		physicalEdgesInOrder.add(edge);

		Integer downStreamvertexID = edge.getTargetId();

		JobVertex headVertex = jobVertices.get(headOfChain);
		JobVertex downStreamVertex = jobVertices.get(downStreamvertexID);

		StreamConfig downStreamConfig = new StreamConfig(downStreamVertex.getConfiguration());

		//下游节点的输入出+1
		downStreamConfig.setNumberOfInputs(downStreamConfig.getNumberOfInputs() + 1);

		//获取分区器
		StreamPartitioner<?> partitioner = edge.getPartitioner();
		if (partitioner instanceof ForwardPartitioner) {
			/**
			 * 这里面会为headVertex创建中间输出集
			 *
			 */
			downStreamVertex.connectNewDataSetAsInput(
				headVertex,
				DistributionPattern.POINTWISE,
				ResultPartitionType.PIPELINED,
				true);
		} else if (partitioner instanceof RescalePartitioner){
			downStreamVertex.connectNewDataSetAsInput(
				headVertex,
				DistributionPattern.POINTWISE,
				ResultPartitionType.PIPELINED,
				true);
		} else {
			downStreamVertex.connectNewDataSetAsInput(
					headVertex,
					DistributionPattern.ALL_TO_ALL,
					ResultPartitionType.PIPELINED,
					true);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("CONNECTED: {} - {} -> {}", partitioner.getClass().getSimpleName(),
					headOfChain, downStreamvertexID);
		}
	}

	/**
	 * 	 上下游的并行度一致
		 下游节点的入度为1 （也就是说下游节点没有来自其他节点的输入）
		 上下游节点都在同一个 slot group 中（下面会解释 slot group）
		 下游节点的 chain 策略为 ALWAYS（可以与上下游链接，map、flatmap、filter等默认是ALWAYS）
		 上游节点的 chain 策略为 ALWAYS 或 HEAD（只能与下游链接，不能与上游链接，Source默认是HEAD）
		 两个节点间数据分区方式是 forward（参考理解数据流的分区）
		 用户没有禁用 chain

	 *
	 *
	 * @param edge
	 * @return
	 */
	private boolean isChainable(StreamEdge edge) {
		StreamNode upStreamVertex = edge.getSourceVertex();
		StreamNode downStreamVertex = edge.getTargetVertex();

		StreamOperator<?> headOperator = upStreamVertex.getOperator();
		StreamOperator<?> outOperator = downStreamVertex.getOperator();

		return downStreamVertex.getInEdges().size() == 1
				&& outOperator != null
				&& headOperator != null
				&& upStreamVertex.isSameSlotSharingGroup(downStreamVertex)
				&& outOperator.getChainingStrategy() == ChainingStrategy.ALWAYS
				&& (headOperator.getChainingStrategy() == ChainingStrategy.HEAD ||
					headOperator.getChainingStrategy() == ChainingStrategy.ALWAYS)
				&& (edge.getPartitioner() instanceof ForwardPartitioner)
				&& upStreamVertex.getParallelism() == downStreamVertex.getParallelism()
				&& streamGraph.isChainingEnabled();
	}

	private void setSlotSharing() {

		Map<String, SlotSharingGroup> slotSharingGroups = new HashMap<>();

		for (Entry<Integer, JobVertex> entry : jobVertices.entrySet()) {

			String slotSharingGroup = streamGraph.getStreamNode(entry.getKey()).getSlotSharingGroup();

			SlotSharingGroup group = slotSharingGroups.get(slotSharingGroup);
			if (group == null) {
				group = new SlotSharingGroup();
				slotSharingGroups.put(slotSharingGroup, group);
			}
			entry.getValue().setSlotSharingGroup(group);
		}

		for (Tuple2<StreamNode, StreamNode> pair : streamGraph.getIterationSourceSinkPairs()) {

			CoLocationGroup ccg = new CoLocationGroup();

			JobVertex source = jobVertices.get(pair.f0.getId());
			JobVertex sink = jobVertices.get(pair.f1.getId());

			ccg.addVertex(source);
			ccg.addVertex(sink);
			source.updateCoLocationGroup(ccg);
			sink.updateCoLocationGroup(ccg);
		}

	}
	
	private void configureCheckpointing() {
		CheckpointConfig cfg = streamGraph.getCheckpointConfig();
		
		if (cfg.isCheckpointingEnabled()) {
			long interval = cfg.getCheckpointInterval();
			if (interval < 1) {
				throw new IllegalArgumentException("The checkpoint interval must be positive");
			}

			// collect the vertices that receive "trigger checkpoint" messages.
			// currently, these are all the sources
			List<JobVertexID> triggerVertices = new ArrayList<>();

			// collect the vertices that need to acknowledge the checkpoint
			// currently, these are all vertices
			List<JobVertexID> ackVertices = new ArrayList<>(jobVertices.size());

			// collect the vertices that receive "commit checkpoint" messages
			// currently, these are all vertices
			List<JobVertexID> commitVertices = new ArrayList<>();
			
			for (JobVertex vertex : jobVertices.values()) {
				if (vertex.isInputVertex()) {
					triggerVertices.add(vertex.getID());
				}
				// TODO: add check whether the user function implements the checkpointing interface
				commitVertices.add(vertex.getID());
				ackVertices.add(vertex.getID());
			}

			JobSnapshottingSettings settings = new JobSnapshottingSettings(
					triggerVertices, ackVertices, commitVertices, interval,
					cfg.getCheckpointTimeout(), cfg.getMinPauseBetweenCheckpoints(),
					cfg.getMaxConcurrentCheckpoints());
			jobGraph.setSnapshotSettings(settings);

			// check if a restart strategy has been set, if not then set the FixedDelayRestartStrategy
			if (streamGraph.getExecutionConfig().getRestartStrategy() == null) {
				// if the user enabled checkpointing, the default number of exec retries is infinitive.
				streamGraph.getExecutionConfig().setRestartStrategy(
					RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, DEFAULT_RESTART_DELAY));
			}
		}
	}

	private void configureRestartStrategy() {
		jobGraph.setRestartStrategyConfiguration(streamGraph.getExecutionConfig().getRestartStrategy());
	}

	// ------------------------------------------------------------------------

	/**
	 * Returns a map with a hash for each {@link StreamNode} of the {@link
	 * StreamGraph}. The hash is used as the {@link JobVertexID} in order to
	 * identify nodes across job submissions if they didn't change.
	 *
	 * <p>The complete {@link StreamGraph} is traversed. The hash is either
	 * computed from the transformation's user-specified id (see
	 * {@link StreamTransformation#getUid()}) or generated in a deterministic way.
	 *
	 * <p>The generated hash is deterministic with respect to:
	 * <ul>
	 * <li>node-local properties (like parallelism, UDF, node ID),
	 * <li>chained output nodes, and
	 * <li>input nodes hashes
	 * </ul>
	 *
	 * @return A map from {@link StreamNode#id} to hash as 16-byte array.
	 */
	private Map<Integer, byte[]> traverseStreamGraphAndGenerateHashes() {
		// The hash function used to generate the hash
		final HashFunction hashFunction = Hashing.murmur3_128(0);
		final Map<Integer, byte[]> hashes = new HashMap<>();

		Set<Integer> visited = new HashSet<>();
		Queue<StreamNode> remaining = new ArrayDeque<>();

		// We need to make the source order deterministic. The source IDs are
		// not returned in the same order, which means that submitting the same
		// program twice might result in different traversal, which breaks the
		// deterministic hash assignment.
		List<Integer> sources = new ArrayList<>();
		for (Integer sourceNodeId : streamGraph.getSourceIDs()) {
			sources.add(sourceNodeId);
		}
		Collections.sort(sources);

		//
		// Traverse the graph in a breadth-first manner. Keep in mind that
		// the graph is not a tree and multiple paths to nodes can exist.
		//

		// Start with source nodes
		for (Integer sourceNodeId : sources) {
			remaining.add(streamGraph.getStreamNode(sourceNodeId));
			visited.add(sourceNodeId);
		}

		StreamNode currentNode;
		while ((currentNode = remaining.poll()) != null) {
			// Generate the hash code. Because multiple path exist to each
			// node, we might not have all required inputs available to
			// generate the hash code.
			if (generateNodeHash(currentNode, hashFunction, hashes)) {
				// Add the child nodes
				for (StreamEdge outEdge : currentNode.getOutEdges()) {
					StreamNode child = outEdge.getTargetVertex();

					if (!visited.contains(child.getId())) {
						remaining.add(child);
						visited.add(child.getId());
					}
				}
			}
			else {
				// We will revisit this later.
				visited.remove(currentNode.getId());
			}
		}

		return hashes;
	}

	/**
	 * Generates a hash for the node and returns whether the operation was
	 * successful.
	 *
	 * @param node         The node to generate the hash for
	 * @param hashFunction The hash function to use
	 * @param hashes       The current state of generated hashes
	 * @return <code>true</code> if the node hash has been generated.
	 * <code>false</code>, otherwise. If the operation is not successful, the
	 * hash needs be generated at a later point when all input is available.
	 * @throws IllegalStateException If node has user-specified hash and is
	 *                               intermediate node of a chain
	 */
	private boolean generateNodeHash(
			StreamNode node,
			HashFunction hashFunction,
			Map<Integer, byte[]> hashes) {

		// Check for user-specified ID
		String userSpecifiedHash = node.getTransformationId();

		if (userSpecifiedHash == null) {
			// Check that all input nodes have their hashes computed
			for (StreamEdge inEdge : node.getInEdges()) {
				// If the input node has not been visited yet, the current
				// node will be visited again at a later point when all input
				// nodes have been visited and their hashes set.
				if (!hashes.containsKey(inEdge.getSourceId())) {
					return false;
				}
			}

			Hasher hasher = hashFunction.newHasher();
			byte[] hash = generateDeterministicHash(node, hasher, hashes);

			if (hashes.put(node.getId(), hash) != null) {
				// Sanity check
				throw new IllegalStateException("Unexpected state. Tried to add node hash " +
						"twice. This is probably a bug in the JobGraph generator.");
			}

			return true;
		}
		else {
			// Check that this node is not part of a chain. This is currently
			// not supported, because the runtime takes the snapshots by the
			// operator ID of the first vertex in a chain. It's OK if the node
			// has chained outputs.
			for (StreamEdge inEdge : node.getInEdges()) {
				if (isChainable(inEdge)) {
					throw new UnsupportedOperationException("Cannot assign user-specified hash "
							+ "to intermediate node in chain. This will be supported in future "
							+ "versions of Flink. As a work around start new chain at task "
							+ node.getOperatorName() + ".");
				}
			}

			Hasher hasher = hashFunction.newHasher();
			byte[] hash = generateUserSpecifiedHash(node, hasher);

			for (byte[] previousHash : hashes.values()) {
				if (Arrays.equals(previousHash, hash)) {
					throw new IllegalArgumentException("Hash collision on user-specified ID. " +
							"Most likely cause is a non-unique ID. Please check that all IDs " +
							"specified via `uid(String)` are unique.");
				}
			}

			if (hashes.put(node.getId(), hash) != null) {
				// Sanity check
				throw new IllegalStateException("Unexpected state. Tried to add node hash " +
						"twice. This is probably a bug in the JobGraph generator.");
			}

			return true;
		}
	}

	/**
	 * Generates a hash from a user-specified ID.
	 */
	private byte[] generateUserSpecifiedHash(StreamNode node, Hasher hasher) {
		hasher.putString(node.getTransformationId(), Charset.forName("UTF-8"));

		return hasher.hash().asBytes();
	}

	/**
	 * Generates a deterministic hash from node-local properties and input and
	 * output edges.
	 */
	private byte[] generateDeterministicHash(
			StreamNode node,
			Hasher hasher,
			Map<Integer, byte[]> hashes) {

		// Include stream node to hash. We use the current size of the computed
		// hashes as the ID. We cannot use the node's ID, because it is
		// assigned from a static counter. This will result in two identical
		// programs having different hashes.
		generateNodeLocalHash(node, hasher, hashes.size());

		// Include chained nodes to hash
		for (StreamEdge outEdge : node.getOutEdges()) {
			if (isChainable(outEdge)) {
				StreamNode chainedNode = outEdge.getTargetVertex();

				// Use the hash size again, because the nodes are chained to
				// this node. This does not add a hash for the chained nodes.
				generateNodeLocalHash(chainedNode, hasher, hashes.size());
			}
		}

		byte[] hash = hasher.hash().asBytes();

		// Make sure that all input nodes have their hash set before entering
		// this loop (calling this method).
		for (StreamEdge inEdge : node.getInEdges()) {
			byte[] otherHash = hashes.get(inEdge.getSourceId());

			// Sanity check
			if (otherHash == null) {
				throw new IllegalStateException("Missing hash for input node "
						+ inEdge.getSourceVertex() + ". Cannot generate hash for "
						+ node + ".");
			}

			for (int j = 0; j < hash.length; j++) {
				hash[j] = (byte) (hash[j] * 37 ^ otherHash[j]);
			}
		}

		if (LOG.isDebugEnabled()) {
			String udfClassName = "";
			if (node.getOperator() instanceof AbstractUdfStreamOperator) {
				udfClassName = ((AbstractUdfStreamOperator<?, ?>) node.getOperator())
						.getUserFunction().getClass().getName();
			}

			LOG.debug("Generated hash '" + byteToHexString(hash) + "' for node " +
					"'" + node.toString() + "' {id: " + node.getId() + ", " +
					"parallelism: " + node.getParallelism() + ", " +
					"user function: " + udfClassName + "}");
		}

		return hash;
	}

	/**
	 * Applies the {@link Hasher} to the {@link StreamNode} (only node local
	 * attributes are taken into account). The hasher encapsulates the current
	 * state of the hash.
	 *
	 * <p>The specified ID is local to this node. We cannot use the
	 * {@link StreamNode#id}, because it is incremented in a static counter.
	 * Therefore, the IDs for identical jobs will otherwise be different.
	 */
	private void generateNodeLocalHash(StreamNode node, Hasher hasher, int id) {
		// This resolves conflicts for otherwise identical source nodes. BUT
		// the generated hash codes depend on the ordering of the nodes in the
		// stream graph.
		hasher.putInt(id);

		hasher.putInt(node.getParallelism());

		if (node.getOperator() instanceof AbstractUdfStreamOperator) {
			String udfClassName = ((AbstractUdfStreamOperator<?, ?>) node.getOperator())
					.getUserFunction().getClass().getName();

			hasher.putString(udfClassName, Charset.forName("UTF-8"));
		}
	}
}
