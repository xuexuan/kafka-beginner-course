package com.start.kafka.first.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoCallbackKey {

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        final Logger logg = LoggerFactory.getLogger(ProducerDemoCallbackKey.class);

        String bootstrapServers = "127.0.0.1:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        for (int i =0; i < 10; ++i)
        {
        	String topic = "first_topic";
        	String value = "hello world"+Integer.toString(i);
        	String key = "id_"+Integer.toString(i);
        	final ProducerRecord<String, String> record = 
        			new ProducerRecord<String, String>(topic, key, value);
        	logg.info("key: "+key);
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null){
                        StringBuilder sb = new StringBuilder();
                        sb.append("received new data. \n");
                        sb.append("Topic" + recordMetadata.topic() + "\n");
                        sb.append("Partition" + recordMetadata.partition() + "\n");
                        sb.append("Offset" + recordMetadata.offset() + "\n");
                        logg.info(sb.toString());
                    }
                    else{
                        logg.error("error", e);
                    }
                }
            }).get();
        }

        producer.flush();
        producer.close();
    }
}
