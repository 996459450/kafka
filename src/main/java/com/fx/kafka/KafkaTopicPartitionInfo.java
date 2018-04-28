package com.fx.kafka;

public class KafkaTopicPartitionInfo {

	public final String topic ;
	public final int partitionID;
	
	public KafkaTopicPartitionInfo(String topic,int partitionID) {
		// TODO Auto-generated constructor stub
		this.topic = topic;
		this.partitionID = partitionID;
	}
	
	@Override
	public boolean equals(Object obj) {
		if(this ==obj) return true;
		if(obj == null || getClass() != obj.getClass()) return false;
		KafkaTopicPartitionInfo that = (KafkaTopicPartitionInfo)obj;
		if(partitionID != that.partitionID) return false;
		
		return topic !=null ? topic.equals(that.topic): that.topic == null;
	}
	
	@Override
	public int hashCode() {
		int result = topic !=null? topic.hashCode() :0;
		result = 31 * result +partitionID;
		return result;
	}
}
