import java.net.*;
import java.lang.Exception;
import java.lang.reflect.Array;
import java.io.*;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.*;

import org.apache.kafka.clients.producer.*;

public class FWQ_Sensor {
	private String id;
	private String ipBroker;
	private String puertoBroker;
	private KafkaProducer<String, Integer> producer;
	private Integer personas;

	public FWQ_Sensor(String ipBroker, String puertoBroker, String id){
		this.ipBroker = ipBroker;
		this.puertoBroker = puertoBroker;
		this.id = id;
	}

	Runnable envioKafka = new Runnable() {
		public void run() {
			ProducerRecord<String, Integer> record = new ProducerRecord<>("Sensores", id ,personas);
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
	public static void main(String[] args) {
		
		if (args.length < 3) {
				System.out.println("Indica: ipBroker puertoBroker id");
				System.exit(1);
		}
		FWQ_Sensor sensor = new FWQ_Sensor(args[0], args[1], args[2]);
		

		Properties kafkaProps = new Properties();
		kafkaProps.put("bootstrap.servers", sensor.ipBroker + ":" + sensor.puertoBroker);

		kafkaProps.put("key.serializer",
			"org.apache.kafka.common.serialization.StringSerializer");
		kafkaProps.put("value.serializer",
			"org.apache.kafka.common.serialization.IntegerSerializer");

		sensor.producer = new KafkaProducer<String, Integer>(kafkaProps);

		ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
		executor.scheduleAtFixedRate(sensor.envioKafka, 0, 3, TimeUnit.SECONDS);
		
		
	}
}
