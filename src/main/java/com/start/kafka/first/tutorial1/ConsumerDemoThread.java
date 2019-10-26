package com.start.kafka.first.tutorial1;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoThread {

	public static void main(String[] args)
	{
		new ConsumerDemoThread().run();
	}
	
	private ConsumerDemoThread() {}
	void run()
	{
		Logger logger = LoggerFactory.getLogger(ConsumerDemoThread.class.getName());
		 String bootstrapServers = "127.0.0.1:9092";
	     Properties properties = new Properties();
	     properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
	     properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
	     properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
	     properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-sixth-application");
	     properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
	     
	     CountDownLatch latch = new CountDownLatch(1);
	     Runnable myConsumerThread = new ConsumerThread(latch ,properties);
	    Thread mythread = new Thread(myConsumerThread);
	    mythread.start();
	    
	    Runtime.getRuntime().addShutdownHook(new Thread(()->{
	    	logger.info("shut down hook");
	    	((ConsumerThread)myConsumerThread).shutdown();
	    	try {
				latch.await();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    }
	    ));
	    
	    try
	    {
	    	latch.await();
	    	
	    }
	    catch(InterruptedException e)
	    {
	    	logger.info("interruptedException");
	    }
	    finally
	    {
	    	logger.info("Application closing");
	    }
	}
	
	public class ConsumerThread implements Runnable
	{
		Logger logger = LoggerFactory.getLogger(ConsumerThread.class.getName());
		CountDownLatch latch ;
		KafkaConsumer<String, String> consumer;
		
		public ConsumerThread(CountDownLatch latch_, Properties properties_)
		{
			this.latch = latch_;
			consumer = new KafkaConsumer<String, String>(properties_);
			consumer.subscribe(Arrays.asList("first_topic"));
		}
		
		public void run() {
			// TODO Auto-generated method stub
			try
			{
				 while(true)
			     {
			    	 ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			    	 for(ConsumerRecord<String, String> record: records)
			    	 {
			    		 logger.info("Key: "+record.key()+" Value "+record.value());
			    		 logger.info("Partition "+record.partition() +" offset "+record.offset());
			    	 }
			     }
			}
			catch(WakeupException e)
			{
				logger.info("shut down");
			}
			finally
			{
				consumer.close();
				latch.countDown();
			}
		}
		
		public void shutdown()
		{
			consumer.wakeup();
		}
		
	}
}
