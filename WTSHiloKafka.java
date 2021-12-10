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

		
		
			KafkaConsumer<String, Integer> consumer = new KafkaConsumer<String, Integer>(props);

			consumer.subscribe(Collections.singletonList("Sensores"));

			Duration timeout = Duration.ofMillis(100);

			while (true) {
				ConsumerRecords<String, Integer> records = consumer.poll(timeout);

				for (ConsumerRecord<String, Integer> record : records) {

					System.out.println("Atracci�n: " + record.key() + ". Personas: " + record.value().toString());
					//l�gica del hilokafka	
					//actualizo el valor "personas" en la bbdd
					try{
						BufferedReader bufrd = new BufferedReader(new FileReader("C:\\kafka\\SD-P2\\atracciones.txt"));
						// TODO Jose:BufferedReader bufrd = new BufferedReader(new FileReader("C:\\kafka\\atracciones.txt"));
						List<String> atracciones = new ArrayList<String>();
						String atraccion = bufrd.readLine();
						while(atraccion != null){
							String[] items = atraccion.split(";");
							if(Integer.parseInt(items[0]) == Integer.parseInt(record.key())){//si es la que nos ha llegado, actualizamos las personas
								items[1] = record.value().toString();
								atraccion = items[0] + ";" + items[1] + ";" + items[2] + ";" + items[3] + ";" + items[4];
							}

							atracciones.add(atraccion);
							atraccion = bufrd.readLine();
						}

						//reescribo el fichero
						try(BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter("C:\\kafka\\SD-P2\\atracciones.txt"))) {
						// TODO Jose: try(BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter("C:\\kafka\\atracciones.txt"))) {
							for(String linea: atracciones){
								bufferedWriter.write(linea + "\n");
							}
						}
						catch (Exception e) {
							System.out.println("Error: " + e.toString());
						}
					}
					catch(Exception e){
						System.out.println("Error: " + e.toString());
					}
				}
			}
		}
		catch(Exception e){
			System.out.println("Exception: " + e.toString());
		}
	}
}



