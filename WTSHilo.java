//hilo que espera las peticiones del engine, para que WTS no se quede bloqueado esperando.
//cuando recibe petición, lee la bbdd y pasa la información a engine
import java.net.*;

public class WTSHilo {
	
	private string puerto;
	
	public MiHiloServidor(string puerto)
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
		ServerSocket ss = new ServerSocket(Integer.parseInt(puerto));
		System.out.println("Escucho el puerto " + puerto);

		for(;;)
		{
			Socket cs = ss.accept();
			System.out.println("Sirviendo al motor...");

			//lógica de servir al engine. (leer fichero con info, enviar datos)

			cs.close();
		}
		
		
}