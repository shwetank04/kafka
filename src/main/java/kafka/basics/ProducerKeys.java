package kafka.basics;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerKeys {
	public static final Logger log = LoggerFactory.getLogger(ProducerKeys.class.getSimpleName());

	public void produce() {
		//Create Producer Properties
		Properties properties = new Properties();
		
		//Local
//		properties.setProperty("bootstrap.servers","127.0.0.1:9092");
		
		//Connect to Conduktor Playground
		properties.setProperty("security.protocol","SASL_SSL");
		properties.setProperty("sasl.mechanism","PLAIN");
		properties.setProperty(
			    "sasl.jaas.config",
			    "org.apache.kafka.common.security.plain.PlainLoginModule required " +
			    "username=\"3ZP2D33l5Fr7rb1t6HlWHB\" " +
			    "password=\"79fa840d-e735-4d48-8131-f2a322fed3c8\";"
			);
		properties.setProperty("bootstrap.servers","cluster.playground.cdkt.io:9092");
		
		properties.setProperty("key.serializer", StringSerializer.class.getName());
		properties.setProperty("value.serializer", StringSerializer.class.getName());

		//Create the Producer
		KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
		
		for(int i=0;i<10;i++) {
			
			String topic = "demo_java";
			String key = "id_" + i;
			String value = "hello world "+i;

			/*If a key is provided →
			Partition = hash(key) % number_of_partitions
			Same key always → same partition (ordering guarantee per key). */
			
			//Create a Producer Record
			//ProducerRecord(String topic, V value)
			ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key,value);
			
			//Send Data
			kafkaProducer.send(producerRecord,new Callback() {
				
				@Override
				public void onCompletion(RecordMetadata metadata, Exception e) {
					//executed every time record is successfully sent or an exception is thrown
					if(e==null) {
						// the record was sent successfully
						log.info("Received new metadata "+metadata.topic() + " "+metadata.partition());
					}
					else {
						log.error("Error while producing");
					}
				}
			});
		}
		
		//flush and close the producer
		kafkaProducer.flush();
		
		kafkaProducer.close();
	}
}
