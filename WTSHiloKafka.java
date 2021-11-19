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
		props.put("value.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");

		try{

		
		
			KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

			consumer.subscribe(Collections.singletonList("Sensores"));

			Duration timeout = Duration.ofMillis(100);

			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(timeout);

				for (ConsumerRecord<String, String> record : records) {

					//TEST
					System.out.printf("Atracción: %s; personas = %d\n", record.key(), record.value());
					//TEST

					//lógica del hilokafka	
					//actualizo el valor "personas" en la bbdd
					try{
						BufferedReader bufrd = new BufferedReader(new FileReader("C:\kafka\SD-P2\atracciones.txt"));
						String line = bufrd.readLine();
						while(line != null){
							String[] items = line.split(";");
							if(items[0] == record.key()){
								//escribir la línea con personas = record.value() (crear una lista de string con las atracciones para luego escribirlas)
							}

							line = bufrd.readLine();
						}
					}
					catch(Exception e){
						System.out.println("Error: " + e.message);
					}

				}
			}
		}
		catch(Exception e){
			System.out.println("Exception: " + e.toString());
		}
	}

}



