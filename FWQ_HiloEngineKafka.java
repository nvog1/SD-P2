import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;
import java.lang.Exception;
import java.io.*;
import java.util.Properties;

public class FWQ_HiloEngineKafka extends Thread {
    private Properties props = new Properties();
    private KafkaConsumer<String, String> consumer;


    public FWQ_HiloEngineKafka() {
        this.props.put("bootstrap.servers", "broker1:9092,broker2:9092");
        this.props.put("group.id", "CountryCounter");
        this.props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        this.props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaConsumer<String, String>(props);
    }

    /*
	// GroupID para diferenciar los topics? (en o'reilly trata el groupID como el topic)
	public String leerKafka(String groupID) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "broker1:9092");
		props.put("group.id", groupID);
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(props);

		// Suscribir el consumer a un topic
		consumer.subscribe(Collections.singletonList(groupID));
	}
    */

    public void run() {

    }
}