package kafka.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumerSample {

	public static void main(String[] args) {

		//Properties for consumer console
		Properties props = new Properties();
		props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group-one");
		props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		// create consumer
		KafkaConsumer<String,String> consumer = new KafkaConsumer<>(props);
		
		//subscribe consumer
		consumer.subscribe(Arrays.asList("firstTopic"));
		
		//poll for data
		while(true) {
			ConsumerRecords<String, String> recs = consumer.poll(Duration.ofMillis(100));
			for(ConsumerRecord<String, String> rec :  recs) {
				System.out.println(rec.key() + ":" + rec.value());
			}
		}
		
	}

}
