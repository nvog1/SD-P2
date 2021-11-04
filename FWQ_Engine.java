import java.net.*;
import java.lang.Exception;
import java.lang.reflect.Array;
import java.net.Socket;
import java.io.*;

public class FWQ_Engine {


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
			String ip_broker;
			String puerto_broker;
			String ip_wts;
			String puerto_wts;
			int maxVisitantes;
			int segundos = 0; //segundos de espera entre peticiones al wts

			ip_broker = args[0];
			puerto_broker = args[1];
			ip_wts = args[2];
			puerto_wts = args[3];
			try{
				maxVisitantes = Integer.parseInt(args[4]);
				segundos = Integer.parseInt(args[5]);
			}
			catch(Exception e){
				System.out.println("error al convertir parámetros");
			}
			

			//conexion a kafka
			//seguramente un thread, aún no sé cómo va kafka
			//tener en cuenta que este hilo va a estar durmiendo 3 segundos
			//cada vez que pida info al WTS

			//conexion a wts
			String mensaje = "";

			for(;;){
				try{
					FWQ_Engine engine = new FWQ_Engine();
					Socket clientSocket = new Socket(ip_wts, Integer.parseInt(puerto_wts));
					mensaje = "1";
					engine.escribeSocket(clientSocket, mensaje);
					mensaje = "";
					mensaje = engine.leeSocket(clientSocket, mensaje);
					//procesar mensaje
					clientSocket.close();
					System.out.println("Conexión cerrada.");
					Thread.sleep(segundos * 1000); //el tiempo lo pide en ms
				}
				catch(Exception e)
				{
					System.out.println("Error: " + e.toString());
				}

			}


		}
		catch(Exception e)
		{
			System.out.println("Error: " + e.toString());
		}


	}
}
