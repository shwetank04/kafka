package wikimedia;

import java.net.URI;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;

public class WikimediaChangesProducer {

    public void wikimediaProducer() throws InterruptedException {

        // Kafka configuration
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        
        //set safe producer config (kafka <=2.8)
        properties.setProperty("enable.idempotence", "true");   // Enables idempotent producer (exactly-once within session)
        properties.setProperty("acks", "all");                  // Leader + all in-sync replicas must ack
        properties.setProperty("retries", Integer.toString(Integer.MAX_VALUE)); // Retry indefinitely

        //set high throughput producer config
        properties.setProperty("linger.ms", "20");                    // Wait up to 20ms before sending batch
        properties.setProperty("batch.size", Integer.toString(32*1024));    // 32 KB batch size
        properties.setProperty("compression.type", "snappy");         // Use snappy compression


        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        String topic = "wikimedia.recentchange";

        // Define event handler: what to do when an event arrives
        BackgroundEventHandler eventHandler = new WikimediaChangeHandler(kafkaProducer, topic);

        // Connect to Wikimedia EventStream
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        EventSource.Builder builder = new EventSource.Builder(URI.create(url));
        BackgroundEventSource eventSource = new BackgroundEventSource.Builder(eventHandler, builder).build();

        // Start streaming in background
        eventSource.start();

        // Keep application running to keep consuming
        Thread.sleep(60000); // run for 1 minute (adjust as needed)

        // Cleanup
        eventSource.close();
        kafkaProducer.close();
    }
}
