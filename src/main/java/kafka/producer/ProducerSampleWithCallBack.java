package kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerSampleWithCallBack {
	
	private static Logger logger = LoggerFactory.getLogger(ProducerSampleWithCallBack.class);
	
	private static void onCompletion(RecordMetadata rec, Exception e) {
		if(e == null) {
			logger.info(""+rec.topic());
		}else {
			logger.error("Error:"+e.getMessage());
		}

	}

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
		producer.send(record, (RecordMetadata rec, Exception e) -> ProducerSampleWithCallBack.onCompletion(rec, e));
		producer.close();
		
	}

}
