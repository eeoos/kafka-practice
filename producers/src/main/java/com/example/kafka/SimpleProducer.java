package com.example.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;

public class SimpleProducer {

	public static void main(String[] args) {
		// KafkaProducer configuration setting

		Properties props = new Properties();
		//bootstrap.servers, key.serializer.class, value.serializer.class
		props.setProperty("bootstrap.servers", "192.168.128.10:9092");
		
	}
}
