package com.fx.kafka;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.OffsetMetadataAndError;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetFetchRequest;
import kafka.javaapi.OffsetFetchResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;

public class KafkaSimpleConsumer {

	//重试次数
	private int maxRetryTimes = 5;
	//重试时间间隔
	private long retryIntervalMillis = 1000;
	//缓存topic/partition对应brokers的连接信息
	private Map<KafkaTopicPartitionInfo, List<KafkaBrokerInfo>> repilcsBrokers = new HashMap<>();
	/**
	 * 运行入口
	 * @param maxReads 最多读取记录数量
	 * @param topic
	 * */
	public void run(long maxReads,KafkaTopicPartitionInfo kafkaTopicPartitionInfo,
			List<KafkaBrokerInfo> seedBrokers) {
		//默认消费数据的偏移量是当前分区的最早偏移量值
		long watchTime = kafka.api.OffsetRequest.EarliestTime();
		String topic = kafkaTopicPartitionInfo.topic;
		int partitionID = kafkaTopicPartitionInfo.partitionID;
		String clientName = this.createclientName(topic, partitionID);
		String groupId = clientName;
		
		//获取当前topic分区对应的分区元数据(包括leader节点的连接信息)
		PartitionMetadata metadata = this.findLeader(seedBrokers, topic, partitionID);
		
		//校验元数据
		this.validatepartitionMetadata(metadata);
		//连接leader节点构建具体的simpleConsumer对象
		SimpleConsumer consumer = this.createSimpleConsumer(metadata.leader().host(), metadata.leader().port(), clientName);
		
		try {
			//获取当前topic、consumer的消费数据offset偏移量
			int times = 0;
			long readOffset = -1;
			while (true) {
				
			}
		} catch (Exception e) {
			// TODO: handle exception
		}
	}
	/**
	 * 获取当前groupId对应consumer在对应topic和partition中对应的offset偏移量
	 * @param consumer 消费者
	 * @param groupId 消费者分区id
	 * @param topic 所属的Topic
	 * @param partitionID 所属的分区
	 * @param whichTime 用于判断，当consumer从没有消费数据的时候，从当前topic的partition的那个offset开始读取数据
	 * @param clientName client名称
	 * @return 正常情况下返回非负数，当出现异常时返回-1
	 * */
	public long getLastOffSet(SimpleConsumer consumer,String groupId,String topic,int partitionID,long whichTime,String clientName) {
		//从zk中获取偏移量，当zk的返回值大于0的时候，表示是一个正常的偏移量
		long offset = this.getOffsetOfTopicAndPartition(consumer, groupId, clientName, topic, partitionID);
		if(offset >0) {
			return offset;
		}
		
		//获取当前topic当前的分区的数据偏移量
		TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partitionID);
		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestinfoMap = new HashMap<TopicAndPartition,PartitionOffsetRequestInfo>();
		requestinfoMap.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, -1));
//		new OffsetRequest(requestinfoMap, 0, clientName);
		return 0;
	}
	
	/**
	 * 从保存consumer消费者offset偏移量的位置获取当前consumer对应的偏移量
	 * @param consumer 消费者
	 * @param groupId group Id
	 * @param clientName client名称
	 * @param topic topic名称
	 * @param partitionID 分区Id
	 * @return 
	 * */
	public long getOffsetOfTopicAndPartition(SimpleConsumer consumer,String groupId,String clientName,String topic,int partitionID) {
		TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partitionID);
		List<TopicAndPartition> requestInfo = new ArrayList<TopicAndPartition>();
		requestInfo.add(topicAndPartition);
		OffsetFetchRequest request = new OffsetFetchRequest(groupId, requestInfo, (short) 0, 0, clientName);
		OffsetFetchResponse response = consumer.fetchOffsets(request);
		//获取返回值
		Map<TopicAndPartition, OffsetMetadataAndError> returOffsetMetadata = response.offsets();
		//处理返回值
		if(returOffsetMetadata != null && !returOffsetMetadata.isEmpty()) {
			//获取当前分区对应的偏移量信息
			OffsetMetadataAndError offset = returOffsetMetadata.get(topicAndPartition);
			if(offset.error() == ErrorMapping.NoError()) {
				//没有异常，表示正常，获取偏移量
				return offset.offset();
			}else {
				//当Consumer第一次连接的时候，会产生UnknowTopicExecptionCode
				System.out.println("Error fetching data Offset Data the topic and Partition.Reason: "+offset.error());
			}
		}
		return 0;
	}
	
	/**
	 * 验证分区元数据，如果验证失败，直接抛出IllegalArgumentException
	 * @param metadata
	 * */
	private void validatepartitionMetadata(PartitionMetadata metadata) {
		if(metadata ==null) {
			System.out.println("can't find metadata for Topic and Partition,Execting!!");
			throw new IllegalArgumentException("can't find metadata for Topic and Partition,Execting!!");
		}
		
		if(metadata.leader() ==null) {
			System.out.println("can't find metadata for Topic and Partition,Execting!!");
			throw new IllegalArgumentException("can't find metadata for Topic and Partition,Execting!!");
		}
	}
	/**
	 *  获取主题和分区对应的主broker节点(即topic和分区id是给定参数的对应brokers节点的元数据)
	 * @param brokers 集群连接参数
	 * @param topic topic名称
	 * @param partitionID 分区ID
	 * @return 
	 * */
	public PartitionMetadata findLeader(List<KafkaBrokerInfo> brokers,String topic,int partitionID) {
		PartitionMetadata returnMetadata = null;
		for(KafkaBrokerInfo broker : brokers) {
			SimpleConsumer consumer = null;
			try {
				//创建简单的消费者对象
				consumer = new SimpleConsumer(broker.brokerHost, broker.brokerPort, 10000, 64*1024, "leaderlookup");
				//构建获取参数的Topic名称参数集合
				List<String> topics = Collections.singletonList(topic);
				//构建请求参数
				TopicMetadataRequest request = new TopicMetadataRequest(topics);
				//请求数据，得到返回对象
				TopicMetadataResponse response = consumer.send(request);
				//获取返回值
				List<TopicMetadata> metadatas = response.topicsMetadata();
				//遍历返回值
				for(TopicMetadata metadata : metadatas) {
					String currentTopic = metadata.topic();
					if(topic.equalsIgnoreCase(currentTopic)) {
						for(PartitionMetadata part : metadata.partitionsMetadata()) {
							if(part.partitionId()==partitionID) {
								//找到对应的元数据
								returnMetadata = part;
								// 更新备份节点的host数据
								if(returnMetadata != null) {
									KafkaTopicPartitionInfo topicPartitionInfo = new KafkaTopicPartitionInfo(topic, partitionID);
									List<KafkaBrokerInfo> brokerInfos = this.repilcsBrokers.get(topicPartitionInfo);
									if(brokerInfos == null) {
										brokerInfos  = new ArrayList<KafkaBrokerInfo>();
									}else {
										brokerInfos.clear();
									}
									
									for(Broker replics : returnMetadata.replicas()) {
										brokerInfos.add(new KafkaBrokerInfo(replics.host(), replics.port()));
									}
									this.repilcsBrokers.put(topicPartitionInfo, brokerInfos);
									return returnMetadata;
								}
							}
						}
					}
				}
				
				
			} catch (Exception e) {
				// TODO: handle exception
				System.out.println("Error communicating with broker ["+broker.brokerHost+"] to find Leader for ["+topic+","+partitionID+"] reason"+e);
			}finally {
				if(consumer != null) {
					consumer.close();
				}
			}
		}
		//没有找到，返回一个空值
		return null;
	}
	
	/**
	 * 构建clientName根据主题名称和分区id
	 * @param topic
	 * @param partitionId
	 * @return 
	 * */
	private String createclientName(String topic ,int partitionID) {
		
		return "clent"+topic+"_"+partitionID;
	}
	
	/**
	 * 根据一个老的consumer，重新创建一个consumer对戏
	 * 
	 * */
	private SimpleConsumer createNewSimpleConsumer(SimpleConsumer consumer,String topic,int partitionID) {
		PartitionMetadata metadata = this.findNewLeaderMetadata(consumer.host(), topic, partitionID);
		this.validatepartitionMetadata(metadata);
		return null;
	}
	
	private PartitionMetadata findNewLeaderMetadata(String oldleader,String topic,int partitionID) {
		KafkaTopicPartitionInfo topicPartitionInfo = new KafkaTopicPartitionInfo(topic, partitionID);
		List<KafkaBrokerInfo> brokerInfos = this.repilcsBrokers.get(topicPartitionInfo);
		for(int i = 0;i< 3;i++) {
			boolean gotoSleep = false;
			PartitionMetadata metadata = this.findLeader(brokerInfos, topic, partitionID);
			if(metadata == null) {
				gotoSleep = true;
			}else if (metadata.leader()== null) {
				gotoSleep = true;
			}else if (oldleader.equalsIgnoreCase(metadata.leader().host()) && i==0) {
				//leader切换过程中
				gotoSleep = true;
			}else {
				return metadata;
			}
			
			if(gotoSleep) {
				try {
					Thread.sleep(1000);
				} catch (Exception e) {
					//nothing to do
				}
			}
		}
		System.out.println("Unable to find new leader after broker failure,exiting!!!");
		throw new RuntimeException("Unable to find new leader after broker failure,exiting!!!");
	}
	
	/**
	 * 构建一个SimpleConsumer并返回
	 * @param host
	 * @param port
	 * @param clientnName
	 * @return
	 * */
	private SimpleConsumer createSimpleConsumer(String host,int port,String clientName) {
		return new SimpleConsumer(host, port, 1000, 64*1024, clientName);
	}
	
	/**
	 * 休眠一段时间
	 * */
	private void sleep() {
		try {
			Thread.sleep(this.maxRetryTimes);
		} catch (Exception e) {
			// TODO: handle exception
		}
	}
	
	/**
	 * 关闭对应资源
	 * @param consumer
	 * */
	private static void closeSimpleConsumer(SimpleConsumer consumer) {
		if(consumer != null) {
			try {
				consumer.close();
			} catch (Exception e) {
				// TODO: handle exception
			}
		}
	}
}
