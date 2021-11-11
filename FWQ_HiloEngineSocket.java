import java.lang.Exception;
import java.net.Socket;
import java.io.*;
import java.time.Duration;

public class FWQ_HiloEngineSocket extends Thread {
    private Socket skCliente;
    
    public FWQ_HiloEngineSocket(Socket p_Cliente) {
        this.skCliente = p_Cliente;
    }

    public String leeSocket(Socket p_sk, String p_Datos) {
        try {
            InputStream aux = p_sk.getInputStream();
            DataInputStream flujo = new DataInputStream(aux);
            p_Datos = new String();
            p_Datos = flujo.readUTF();
        }
        catch (Exception e) {
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
        /*try{
			FWQ_Engine engine = new FWQ_Engine();
			Socket clientSocket = new Socket(ip_wts, Integer.parseInt(puerto_wts));
			mensaje = "1";
			engine.escribeSocket(clientSocket, mensaje);
			mensaje = "";
			mensaje = engine.leeSocket(clientSocket, mensaje);
			//procesar mensaje
			clientSocket.close();
			System.out.println("Conexi�n cerrada.");
			Thread.sleep(segundos * 1000); //el tiempo lo pide en ms
		}
		catch(Exception e) {
			System.out.println("Error: " + e.toString());
		}*/

        /*String resultado = "0";
        String cadena = "";

        try {
            while (resultado != "") {
                cadena = this.leeSocket(skCliente, cadena);
                
                resultado = this.resolverCifrado(cadena);
                cadena = "" + resultado;
                this.escribeSocket(skCliente, cadena);
            }
            skCliente.close();
        }
        catch (Exception e) {
            System.out.println("Error: " + e.toString());
        }*/
    }
}