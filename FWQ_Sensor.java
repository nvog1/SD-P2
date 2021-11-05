import java.net.*;
import java.lang.Exception;
import java.lang.reflect.Array;
import java.io.*;
import java.util.Properties;

import org.apache.kafka.clients.producer.*;

public class FWQ_Sensor {
	private String id;
	private int tiempoCiclo;
	private String nombre;

	public FWQ_Sensor(String nombre, String id, int tiempoCiclo){
		this.nombre = nombre;
		this.id = id;
		this.tiempoCiclo = tiempoCiclo;
	}

	/**
	 * @param args
	 */
	public void main(String[] args) {
		
		
		
	}
}
