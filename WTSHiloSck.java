//hilo que espera las peticiones del engine, para que WTS no se quede bloqueado esperando.
//cuando recibe petición, lee la bbdd y pasa la información a engine
import java.net.*;
import java.lang.Exception;
import java.lang.reflect.Array;
import java.net.Socket;
import java.io.*;
import java.util.*;

public class WTSHiloSck extends Thread{
	
	private String puerto;
	
	public WTSHiloSck(String puerto)
	{
		this.puerto = puerto;
	}


	public String leeSocket (Socket p_sk, String p_Datos)
	{
		try
		{
			InputStream aux = p_sk.getInputStream();
			DataInputStream flujo = new DataInputStream( aux );
			p_Datos = new String();
			p_Datos = flujo.readUTF();
		}
		catch (Exception e)
		{
			System.out.println("Error: " + e.toString());
		}
      return p_Datos;
	}

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

	public void run() {
		try{
			ServerSocket ss = new ServerSocket(Integer.parseInt(puerto));
			System.out.println("Escucho el puerto " + puerto);

			for(;;)
			{
				Socket cs = ss.accept();
				System.out.println("Sirviendo al motor...");

				//lógica de servir al engine. (leer fichero con info, enviar datos)
				try{
					BufferedReader bufrd = new BufferedReader(new FileReader("C:\\kafka\\SD-P2\\atracciones.txt"));
					List<String> atracciones = new ArrayList<String>();
					String atraccion = bufrd.readLine();
					while(atraccion != null){
						String[] items = atraccion.split(";");
						// tiempoEspera = (personas / personasCiclo) * tiempoCiclo
						float tiempoEspera = (Integer.parseInt(items[1]) / Integer.parseInt(items[2]) ) * Integer.parseInt(items[3]);
						atraccion = items[0] + ";" + items[4] + ";" + items[5] + ";" + tiempoEspera;

						atracciones.add(atraccion);
						atraccion = bufrd.readLine();
					}

					//envío la info
					String mensaje = "";
					for(String linea: atracciones){
						mensaje += linea + "\n";
					}

					escribeSocket(cs, mensaje);
					
				}
				catch(Exception e){
					System.out.println("Error: " + e.toString());
				}

				cs.close();
			}
		}
		catch(Exception e){
			System.out.println("Error:" + e.toString());
		}
		
	}		
}