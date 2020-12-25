package kafka.consumer;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

public class ConsumerThread implements Runnable {

	private CountDownLatch latch;
	private KafkaConsumer<String, String> consumer;

	public KafkaConsumer<String, String> getConsumer(){
		return this.consumer;
	}

	ConsumerThread(CountDownLatch latch, Properties props){
		this.latch=latch;
		this.consumer =  new KafkaConsumer<>(props);
	}

	@Override
	public void run() {
		try {
		while(true) {
			ConsumerRecords<String, String> recs = this.consumer.poll(Duration.ofMillis(100));
			for(ConsumerRecord<String, String> rec :  recs) {
				System.out.println(rec.key() + ":" + rec.value());
			}
		}
		}catch(WakeupException wue) {
			System.out.println("I have been woken up");
		}finally {
			this.consumer.close();
			latch.countDown();
		}
	}

	public void shutdown() {
		this.consumer.wakeup();
	}

}
