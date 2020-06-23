package com.jsb.collector.kafka;

import java.util.Properties;

import org.springframework.beans.factory.annotation.Value;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class KafkaProducerService {
	
	@Value("${kafka.topic.thetechcheck}")
    private String theTechCheckTopicName;

    @Value("${kafka.bootstrap.servers}")
    private String kafkaBootstrapServers;

    @Value("${zookeeper.groupId}")
    private String zookeeperGroupId;

    @Value("${zookeeper.host}")
    String zookeeperHost;
	
	private static final Logger logger = LogManager.getLogger(KafkaProducerService.class);
	
	// Producer
	public KafkaProducer<String, String> producer;
	
	public KafkaProducerService() {
		Properties producerProperties = createProducerProperties();
		this.producer = new KafkaProducer<>(producerProperties);
	}
	
	/**
	 * Example to produce information from Kafka
	 * @param producer
	 */
	private void sendInformationToKafka(KafkaProducer<String, String> producer) {
		for (int index = 0; index < 10; index++) {
            sendKafkaMessage("The index is now: " + index, producer, theTechCheckTopicName);
        }
	}
	
	/**
	 * Function to send a message to Kafka
	 * @param payload The String message that we wish to send to the Kafka topic
	 * @param producer The KafkaProducer object
	 * @param topic The topic to which we want to send the message
	 */
	private static void sendKafkaMessage(String payload,
			KafkaProducer<String, String> producer,
	         String topic)
	{
	    logger.info("Sending Kafka message: " + payload);
	    producer.send(new ProducerRecord<>(topic, payload));
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
