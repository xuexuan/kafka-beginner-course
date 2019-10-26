package com.start.kafka.first.tutorial1;

import java.lang.reflect.Array;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoAsignSeek {

	static Logger logger = LoggerFactory.getLogger(ConsumerDemoAsignSeek.class.getName());
	
	public static void main(String[] args)
	{
		 String bootstrapServers = "127.0.0.1:9092";
	     Properties properties = new Properties();
	     properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
	     properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
	     properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
	     properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
	     
	     KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties); 
	     //consumer.subscribe(Arrays.asList("first_topic"));
	     TopicPartition partitionFrom = new TopicPartition("first_topic", 0);
	     consumer.assign(Arrays.asList(partitionFrom));
	     consumer.seek(partitionFrom, 15L);
	     
	     for(int i = 0; i < 5;++i)
	     {
	    	 ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
	    	 for(ConsumerRecord<String, String> record: records)
	    	 {
	    		 logger.info("Key: "+record.key()+" Value "+record.value());
	    		 logger.info("Partition "+record.partition() +" offset "+record.offset());
	    	 }
	     }
	}
}
