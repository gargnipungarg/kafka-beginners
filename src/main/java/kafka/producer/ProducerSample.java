package kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerSample {

	public static void main(String[] args) {

		// Create producer property
		Properties props = new Properties();
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		// Create producer
		KafkaProducer<String,String> producer = new KafkaProducer<>(props);
		
		// Create producer record
		ProducerRecord<String,String> record = new ProducerRecord<String, String>("firstTopic", "Hey from Java");
		
		// Send data
		producer.send(record);
		producer.close();
		
	}

}
