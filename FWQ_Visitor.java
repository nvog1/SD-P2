import java.io.*;
import java.net.*;

public class FWQ_Visitor {

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

    // Lo que haga el servidor a la hora de registrar un visitante
    
    /*
    * Crear perfil
    */


    /*
    * Editar perfil
    */

    /*
    * Entrar al parque
    */

    /*
    * Salir del parque
    */

    public static void main(String[] args) {
        FWQ_Visitor visitante = new FWQ_Visitor();
        String RegistryHost;
        String RegsitryPort;
        String QueueHandlerHost;
        String QueueHandlerPort;
        if (args.length < 4) {
            System.out.println("Se debe indicar la direccion y el puerto del registro" + 
            " y del gestor de colas");
            System.out.println("");
        }
    }
}
