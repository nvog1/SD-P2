import java.net.*;
import java.lang.Object;
import java.lang.Exception;
import java.lang.reflect.Array;
import java.net.Socket;
import java.io.*;
import java.time.Duration;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

public class FWQ_Engine {
	private static String ipWTS;
	private static Integer puertoWTS;
	// En atracciones se guarda: ID; posX; posY; tiempoEspera; tiempoCiclo
	private static volatile String atracciones;


	/*
	* Lee datos del socket. Supone que se le pasa un buffer con hueco 
	*	suficiente para los datos. Devuelve el numero de bytes leidos o
	* 0 si se cierra fichero o -1 si hay error.
	*/
	public String leeSocket (Socket p_sk, String p_Datos)
	{
		try
		{
			InputStream aux = p_sk.getInputStream();
			DataInputStream flujo = new DataInputStream( aux );
			p_Datos = flujo.readUTF();
		}
		catch (SocketException e) {
			System.out.println("Se ha perdido la conexion");
		}
		catch (Exception e)
		{
			System.out.println("Error: " + e.toString());
		}
      return p_Datos;
	}

	/*
	* Escribe dato en el socket cliente. Devuelve numero de bytes escritos,
	* o -1 si hay error.
	*/
	public void escribeSocket (Socket p_sk, String p_Datos)
	{
		try
		{
			OutputStream aux = p_sk.getOutputStream();
			DataOutputStream flujo= new DataOutputStream( aux );
			flujo.writeUTF(p_Datos);      
		}
		catch (Exception e)
		{
			System.out.println("Error: " + e.toString());
		}
		return;
	}

	//hilo que hace request al WTS para saber el estado de las atracciones
	Runnable sckRequest = new Runnable() {
		public void run() {
			String mensaje = "";
			try{
				Socket skCliente = new Socket(FWQ_Engine.ipWTS, FWQ_Engine.puertoWTS);
				mensaje = leeSocket(skCliente, mensaje);
				FWQ_Engine.atracciones = mensaje;
			}
			catch(Exception e){
				System.out.println("Error: " + e.toString());
			}

			//DEBUG
			System.out.println(mensaje);
			//DEBUG
			
		}
	};

	public static String getAtracciones() {
		return atracciones;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		try
		{
			if (args.length < 6) {
				System.out.println("Indica: ip_broker puerto_broker ip_wts puerto_wts maxVisitantes segundos");
				System.exit(1);
			}
			String ip_broker = "";
			String puerto_broker = "";
			//String ip_wts = "";
			//String puerto_wts = "";
			int maxVisitantes = -1;
			int segundos = -1; //segundos de espera entre peticiones al wts

			ip_broker = args[0];
			puerto_broker = args[1];
			FWQ_Engine.ipWTS = args[2];
			try{
				FWQ_Engine.puertoWTS = Integer.parseInt(args[3]);
				maxVisitantes = Integer.parseInt(args[4]);
				segundos = Integer.parseInt(args[5]);
			}
			catch(Exception e){
				System.out.println("Error al convertir par�metros");
			}
			

			// Hilo de kafka
			Thread tKafka = new FWQ_HiloEngineKafka(ip_broker, puerto_broker, maxVisitantes, segundos);
			tKafka.start();

			//conexion a wts
			FWQ_Engine engine = new FWQ_Engine();

			// Hilo de Sockets
			ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
			executor.scheduleAtFixedRate(engine.sckRequest, 0, segundos, TimeUnit.SECONDS);
		}
		catch(Exception e)
		{
			System.out.println("Error: " + e.toString());
		}


	}
}
