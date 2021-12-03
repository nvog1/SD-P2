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

	private class ProducerCallback implements Callback {
		@Override
		public void onCompletion(RecordMetadata recordMetadata, Exception e) {
			if (e != null) {
				System.out.println("Mensaje no recibido correctamente, puede que haya caído el broker o el WTS");
			}
		}
	}

	public FWQ_Sensor(String ipBroker, String puertoBroker, String id){
		this.ipBroker = ipBroker;
		this.puertoBroker = puertoBroker;
		this.id = id;
	}

	Runnable envioKafka = new Runnable() {
		public void run() {
			//revisar simulación llegada para hacerla más realista
			Random rd = new Random();
			personas = rd.nextInt(50);
			ProducerRecord<String, Integer> record = new ProducerRecord<>("Sensores", id, personas);
			try {
				producer.send(record, new ProducerCallback());
				System.out.println("Atracción " + id + ": " + personas);
			} 
			catch (Exception e) {
				e.printStackTrace();
			}
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
		kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
		kafkaProps.put("max.block.ms", "1000");
		kafkaProps.put("delivery.timeout.ms", "1900");
		kafkaProps.put("linger.ms", "0");
		kafkaProps.put("request.timeout.ms", "50");

		sensor.producer = new KafkaProducer<String, Integer>(kafkaProps);

		ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
		executor.scheduleAtFixedRate(sensor.envioKafka, 0, 3, TimeUnit.SECONDS);
		
		
	}
}
