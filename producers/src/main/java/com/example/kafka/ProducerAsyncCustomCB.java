package com.example.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerAsyncCustomCB {
	public static final Logger logger = LoggerFactory.getLogger(ProducerAsyncCustomCB.class.getName());
	public static void main(String[] args) {

		String topicName = "multipart-topic";

		// KafkaProducer configuration setting
		// null, "hello world"

		Properties props = new Properties();
		//bootstrap.servers, key.serializer.class, value.serializer.class
		props.setProperty("bootstrap.servers", "192.168.128.10:9092");
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.128.10:9092");
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// KafkaProducer object creation
		// network thread가 만들어짐
		KafkaProducer<Integer, String> kafkaProducer = new KafkaProducer<>(props);


		for (int seq=0; seq < 20; seq++) {

			//ProducerRecord object creation
			ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(topicName, seq, "hello world " + seq);
			Callback callback = new CustomCallback(seq);
			// logger.info("seq:" + seq);
			// KafkaProducer message send
			// serializer하고 partitioner 하고 그 후에
			// network thread에게  record를 전달
			kafkaProducer.send(producerRecord, callback);
		}
		try {
			Thread.sleep(3000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		kafkaProducer.close();
	}
}
