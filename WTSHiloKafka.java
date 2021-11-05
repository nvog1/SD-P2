import java.net.*;
import java.lang.Exception;
import java.lang.reflect.Array;
import java.net.Socket;
import java.io.*;
import java.util.Properties;
import java.util.*;
import java.time.*;

import org.apache.kafka.clients.consumer.* ;

public class WTSHiloKafka extends Thread {

	private String ipBroker;
	private String puertoBroker;
	
	public WTSHiloKafka(String ipBroker, String puertoBroker){
		this.ipBroker = ipBroker;
		this.puertoBroker = puertoBroker;
	}

	public void run(){
		Properties props = new Properties();
		props.put("bootstrap.servers", ipBroker + ":" + puertoBroker);
		props.put("group.id", "WTS");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

		consumer.subscribe(Collections.singletonList("Sensores"));

		Duration timeout = Duration.ofMillis(100);

		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(timeout);

			for (ConsumerRecord<String, String> record : records) {

				//TEST
				System.out.printf("topic = %s, partition = %d, offset = %d, " +
								"customer = %s, country = %s\n",
				record.topic(), record.partition(), record.offset(),
						record.key(), record.value());
				//TEST

				//lógica del hilokafka	
			}
		}
	}

}



