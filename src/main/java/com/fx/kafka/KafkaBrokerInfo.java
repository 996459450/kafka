package com.fx.kafka;

/**
 * kafka 连接参数类
 * */
public class KafkaBrokerInfo {

	public final String brokerHost;
	
	public final int brokerPort;
	
	public KafkaBrokerInfo(String brokerHost,int brokerPort) {
		// TODO Auto-generated constructor stub
		this.brokerHost = brokerHost;
		this.brokerPort = brokerPort;
	}

	public KafkaBrokerInfo(String brokerHost) {
		// TODO Auto-generated constructor stub
		this(brokerHost, 9092);
	}
	
}
