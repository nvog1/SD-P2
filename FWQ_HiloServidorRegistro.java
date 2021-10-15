import java.lang.Exception;
import java.net.Socket;
import java.io.*;

public class HiloServidorRegistro extends Thread {
    /* TODO
    / Campos implementables:
    /  · Registro de clientes con nombre, coordenadas actuales y destino
    */

    private Socket skCliente;

    public HiloServidorRegistro(Socket p_Cliente) {
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

    public int realizarRegistro(String cadena) {
        // El caracter de separacion sera el punto y coma ';'
        // Se guardara Alias/ID, Nombre y contraseña

        String[] operacion = cadena.split(";");
        int result = 0;

        if (operacion.length == 4) {
            System.out.println("SRV: Alias/ID: " + operacion[1] + "; Nombre: " + operacion[2] + 
            "; Contraseña: " + operacion[3]);
            
            //-------------------------------//
            // Conexion con la base de datos //
            //-------------------------------//

            result = 1;
        }
        else {
            System.out.println("No se han proporcionado los campos necesarios");
            result = -1;
        }

        return result;
    }

    public void run() {
        int resultado = 0;
        String cadena = "";

        try {
            while (resultado != -1) {
                cadena = this.leeSocket(skCliente, cadena);
                resultado = this.realizarRegistro(cadena);
                cadena = "" + resultado;
                this.escribeSocket(skCliente, cadena);
            }
            skCliente.close()
        }
        catch (Exception e) {
            System.out.println("Error: " + e.toString());
        }
    }
}
