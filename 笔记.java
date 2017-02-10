/**
 时间类型：
 eventTime ：用户赋予的自定义的时间戳（事件时间戳）
 processingTime ： 执行当前task的subtask主机的本地时间戳（系统时间戳）


 StreamNode的编号id的生成是通过调用StreamTransformation的静态方法getNewNodeId获得的，其实现是一个静态计数器

 StreamEdge的编号edgeId是字符串类型，其生成的规则为：
 this.edgeId = sourceVertex + "_" + targetVertex + "_" + typeNumber + "_" + selectedNames + "_" + outputPartitioner;


 StreamOperator相当于Function的运行环境,绝大多数实现都是继承OneInputStreamOperator，有一个processElement和processWaterMark
 但是StreamSource不是继承自它，他有一个run方法来执行Function

 StreamTransformation聚合了StreamOperator，提供输入、并行度、名字、slotSharingGroup等。暂时就这样理解

 startNewChain()来指示从该operator开始一个新的chain（与前面截断，不会被chain到前面）

 ResultPartitionType
 其中管道属性会对消费端任务的消费行为产生很大的影响。如果是管道型的，那么在结果分区接收到第一个Buffer时，
 消费者任务就可以进行准备消费（如果还没有部署则会先部署），而如果非管道型，那么消费者任务将等到生产端任务生产完数据之后才会着手进行消费。



 IntermediateResultPartition(ExecutionGraph的表示)是IntermediateDataSet（中间结果集,JobGraph的表示）的一个分区，
 无混淆的说就是并行度是多少就有多少个。
 一个ExecutionVertex会有多个IntermediateResultPartition，因为一个jobVertex有多个下游jobVertex。
 IntermediateResultPartition也有多个分区



 ResultPartition（物理执行图表示）和IntermediateResultPartition对应

 IntermediateResult和IntermediateDataSet对应




 InputGate消费多个ResultPartition，意思是InputGate的inputChannel消费ResultSubpartition。

 JobVertex有多个输入IntermediateDataSet和多个输出中间结果集，注意这个输入输出的是JobVertex而不是某个分区。
 即JobVertex的这两个字段：
	private final ArrayList<IntermediateDataSet> results = new ArrayList<IntermediateDataSet>();
	private final ArrayList<JobEdge> inputs = new ArrayList<JobEdge>();
    JobEdge里面有输入的JobVertex和它的IntermediateDataSet。
 那么什么时候JobVertex会有多个输入呢？ 就是Union时，当在StreamGraph时，某个OneInput转换的上游转换是Union时
 Collection<Integer> inputIds = transform(transform.getInput());就是这句代码会得到两个输入
 最后：StreamEdge edge = new StreamEdge(upstreamNode, downstreamNode, typeNumber, outputNames, partitioner);
	 getStreamNode(edge.getSourceId()).addOutEdge(edge);
	 getStreamNode(edge.getTargetId()).addInEdge(edge);
 等构建JobGraph时这个job顶点就有多个输入了。

 什么时候有多个输出呢，就是迭代流，既要输出给反馈，又要往下游输出




 In case of a small data set R and a much larger data set S, broadcasting R and using it as
 build-side input of a Hybrid-Hash-Join is usually a good choice because the much larger data set S is
 not shipped and not materialized (given that the hash table completely fits into memory). If both data
 sets are rather
 large or the join is performed on many parallel instances, repartitioning both inputs is a robust choice.









































 /
