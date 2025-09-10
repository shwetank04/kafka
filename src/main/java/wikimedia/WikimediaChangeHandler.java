package wikimedia;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.launchdarkly.eventsource.MessageEvent;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;

public class WikimediaChangeHandler implements BackgroundEventHandler {
	
	KafkaProducer<String, String> kafkaProducer; 
	String topic;
	
	public WikimediaChangeHandler(KafkaProducer<String, String> kafkaProducer,String topic) {
		this.kafkaProducer = kafkaProducer;
		this.topic = topic;
	}

	@Override
	public void onOpen() throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onClosed() throws Exception {
		// TODO Auto-generated method stub
		kafkaProducer.close();
		
	}

	@Override
	public void onMessage(String event, MessageEvent messageEvent) throws Exception {
		// ASYNC 
		kafkaProducer.send(new ProducerRecord<>(topic,messageEvent.getData()));
	}

	@Override
	public void onComment(String comment) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onError(Throwable t) {
		// TODO Auto-generated method stub
		
	}

}
