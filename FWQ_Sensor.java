import java.net.*;
import java.lang.Exception;
import java.lang.reflect.Array;
import java.io.*;
import java.util.Properties;

import org.apache.kafka.clients.producer.*;

public class FWQ_Sensor {
	private String id;
	private String ipBroker;
	private String puertoBroker;

	public FWQ_Sensor(String ipBroker, String puertoBroker, int id){
		this.ipBroker = ipBroker;
		this.puertoBroker = puertoBroker;
		this.id = id;
	}

	/**
	 * @param args
	 */
	public void main(String[] args) {
		
		if (args.length < 3) {
				System.out.println("Indica: ipBroker puertoBroker id");
				System.exit(1);
		}

		for(;;){
			
			//enviar número de personas en la cola cada 1-3 segundos(random)
		}
		
	}
}
