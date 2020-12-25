package kafka.consumer;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumerSampleWithThread {

	public static void main(String[] args) {

		//Properties for consumer console
		Properties props = new Properties();
		props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group-two");
		props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		// create consumer
		
		
		ConsumerThread consumerThread = new ConsumerThread(new CountDownLatch(1), props);
		Thread t = new Thread(new ConsumerThread(new CountDownLatch(1), props));
		KafkaConsumer<String,String> consumer = consumerThread.getConsumer();
		
		//subscribe consumer
		consumer.subscribe(Arrays.asList("firstTopic"));
		// add latch shut down hook to properly handle shutdown
		
		//poll for data
		t.start();
		
	}

}
