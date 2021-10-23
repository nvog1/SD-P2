import java.net.*

public class FWQ_Engine {

	private string ip_broker;
	private string puerto_broker;
	private string ip_wts;
	private string puerto_wts;
	private int maxVisitantes;
	private int segundos;//segundos de espera entre peticiones al wts

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
			ip_broker = args[0];
			puerto_broker = args[1];
			ip_wts = args[2];
			puerto_wts = args[3];
			maxVisitantes = args[4];
			segundos = args[5];

			//conexion a kafka
			//seguramente un thread, aún no sé cómo va kafka
			//tener en cuenta que este hilo va a estar durmiendo 3 segundos
			//cada vez que pida info al WTS

			//conexion a wts
			String mensaje = "";

			for(;;;){
				try{
					Socket clientSocket = new Socket(ip_wts, Integer.parseInt(puerto_wts));
					mensaje = "1";
					escribeSocket(clientSocket, mensaje);
					mensaje = "";
					mensaje = leeSocket(clientSocket, mensaje);
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
