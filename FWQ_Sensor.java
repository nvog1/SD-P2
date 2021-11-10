import java.net.*;
import java.lang.Exception;
import java.lang.reflect.Array;
import java.io.*;
import java.util.Properties;
import java.util.random;
import java.time.duration;
import java.util.concurrent;

import org.apache.kafka.clients.producer.*;

public class FWQ_Sensor {
	private String id;
	private String ipBroker;
	private String puertoBroker;
	private KafkaProducer<String, String> producer;
	private int personas;

	public FWQ_Sensor(String ipBroker, String puertoBroker, int id){
		this.ipBroker = ipBroker;
		this.puertoBroker = puertoBroker;
		this.id = id;
	}

	Runnable envioKafka = new Runnable() {
		public void run() {
			ProducerRecord<String, int> record = new ProducerRecord<>("Sensores", this.id ,personas);
			try {
				producer.send(record);
			} 
			catch (Exception e) {
				e.printStackTrace();
			}
			//lógica de enviar mensaje a kafka
		}
	};



	/**
	 * @param args
	 */
	public void main(String[] args) {
		
		if (args.length < 3) {
				System.out.println("Indica: ipBroker puertoBroker id");
				System.exit(1);
		}
		ipBroker = args[0];
		puertoBroker = args[1];
		id = args[2];

		Properties kafkaProps = new Properties();
		kafkaProps.put("bootstrap.servers", ipBroker + ":" + puertoBroker);

		kafkaProps.put("key.serializer",
			"org.apache.kafka.common.serialization.StringSerializer");
		kafkaProps.put("value.serializer",
			"org.apache.kafka.common.serialization.IntegerSerializer");

		this.producer = new KafkaProducer<String, int>(kafkaProps);

		ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
		executor.scheduleAtFixedRate(envioKafka, 0, 3, TimeUnit.SECONDS);
		
		
	}
}
