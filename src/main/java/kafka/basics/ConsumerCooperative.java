package kafka.basics;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerCooperative {

    public static final Logger log = LoggerFactory.getLogger(ConsumerCooperative.class.getSimpleName());

    
	public void consume() {

		//Create Consumer Properties

		Properties properties = new Properties();
        String groupId = "my-java-application";
        String topic = "demo_java";

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

        //Create Consumer configs
		properties.setProperty("key.deserializer", StringDeserializer.class.getName());
		properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        // properties.setProperty("auto.offset.reset", none/earliest/latest); Total 3 options
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());

        //Start consuming from the beginning of the partition.
        properties.setProperty("auto.offset.reset", "earliest");

        //Create Consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);

        //Get a reference to the main thread
        final Thread mainThread = Thread.currentThread();

        //Add a shutdown hook, this is called when application is stopped
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                log.info("Detected shutdown, lets exit by calling consumer.wakeup()");
                consumer.wakeup(); //Trigger an exception on consumer while it is consuming

                //Join the main thread to allow the execution of code in main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        });

        try {
        //Subscribe to a topic
        consumer.subscribe(Arrays.asList(topic));

        //poll for data
        while(true) {
             ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(1000));

            for(ConsumerRecord<String,String> record : records) {
                log.info("Key: "+record.key()+ " "+"Value "+record.value());
            }
            }
        }
        catch(WakeupException e){
            log.info("Consumer is starting to shutdown");
        } catch(Exception e){
            log.error("Unexpected exception");
        }
        finally{
            consumer.close(); //close the consumer, this will also commit offset
            log.info("The consumer is now gracefully shut down");
        }
    }   
}
