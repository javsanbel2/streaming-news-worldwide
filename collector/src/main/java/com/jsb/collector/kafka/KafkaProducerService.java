package com.jsb.collector.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducerService {

	@Value("${kafka.bootstrap.servers}")
	private String kafkaBootstrapServers;

	@Value("${zookeeper.groupId}")
	private String zookeeperGroupId;

	@Value("${zookeeper.host}")
	private String zookeeperHost;

	private static final Logger logger = LogManager.getLogger(KafkaProducerService.class);

	private KafkaProducer<String, String> producer = new KafkaProducer<>(createProducerProperties());

	/**
	 * Function to send a message to Kafka
	 * 
	 * @param payload  The String message that we wish to send to the Kafka topic
	 * @param producer The KafkaProducer object
	 * @param topic    The topic to which we want to send the message
	 */
	public void sendKafkaMessage(String topic, String payload) {
		logger.info("Sending Kafka message: " + payload);
		this.producer.send(new ProducerRecord<>(topic, payload));
	}

	/**
	 * Defining producer properties.
	 */
	private static Properties createProducerProperties() {

		Properties producerProperties = new Properties();
		producerProperties.put("bootstrap.servers", "localhost:9092");
		producerProperties.put("acks", "all");
		producerProperties.put("retries", 0);
		producerProperties.put("batch.size", 16384);
		producerProperties.put("linger.ms", 1);
		producerProperties.put("buffer.memory", 33554432);
		producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		return producerProperties;
	}

}
