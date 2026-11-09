package com.example.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

public class SimpleProducer {

	public static void main(String[] args) {
		// KafkaProducer configuration setting
		// null, "hello world"

		Properties props = new Properties();
		//bootstrap.servers, key.serializer.class, value.serializer.class
		props.setProperty("bootstrap.servers", "192.168.128.10:9092");
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.128.10:9092");
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		
	}
}
