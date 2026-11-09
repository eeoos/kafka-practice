package com.example.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class SimpleProducer {

	public static void main(String[] args) {

		String topicName = "simple-topic";

		// KafkaProducer configuration setting
		// null, "hello world"

		Properties props = new Properties();
		//bootstrap.servers, key.serializer.class, value.serializer.class
		props.setProperty("bootstrap.servers", "192.168.128.10:9092");
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.128.10:9092");
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// KafkaProducer object creation
		// network thread가 만들어짐
		KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

		//ProducerRecord object creation
		ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, "hello world 2");

		// KafkaProducer message send
		// serializer하고 partitioner 하고 그 후에
		// network thread에게  record를 전달
		kafkaProducer.send(producerRecord);

		kafkaProducer.flush();
		kafkaProducer.close();
	}
}
