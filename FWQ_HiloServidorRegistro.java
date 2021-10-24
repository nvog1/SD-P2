import java.lang.Exception;
import java.net.Socket;
import java.io.*;
import java.sql.*;

public class FWQ_HiloServidorRegistro extends Thread {
    /* TODO
    / Campos implementables:
    /  · Registro de clientes con nombre, coordenadas actuales y destino
    */

    private Socket skCliente;

    public FWQ_HiloServidorRegistro(Socket p_Cliente) {
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

    // Conexion con la Base de datos
    // URL de Conexion
    /*String connectionURL = "jdbc:mysql://localhost:3306/NOMBRE_BD";
    String user = "root";
    String password = "1234";*/

    // Realizar una consulta a la base de datos
    public boolean consultaSQL() {
        String connectionURL = "jdbc:mysql://localhost:3306/NOMBRE_BD";
        String user = "root";
        String password = "1234";
        try (Connection connection = DriverManager.getConnection(connectionURL, user, password)) {
            
        }
        catch (SQLException e) {
            System.out.println("Error SQL: " + e.getMessage());
        }
        
        return true;
    }

    public int realizarRegistro(String cadena) {
        // El caracter de separacion sera el punto y coma ';'
        // Se guardara Alias/ID, Nombre y contraseņa

        String[] operacion = cadena.split(";");
        int result = 0;

        if (operacion.length == 4) {
            System.out.println("SRV: se va a " + operacion[0] + "un perfil con los siguientes datos" +
            " Alias/ID: " + operacion[1] + "; Nombre: " + operacion[2] + 
            "; Contraseņa: " + operacion[3]);
            
            //-------------------------------//
            // Conexion con la base de datos //
            //    mediante capas EN y CAD    // 
            //-------------------------------//
            // Pagina que explica como conectar y hacer consultas:
            //   https://docs.microsoft.com/es-es/sql/connect/jdbc/step-3-proof-of-concept-connecting-to-sql-using-java?view=sql-server-ver15


            result = 1;
        }
        // Comprobar si hace falta poner aqui las operaciones de entrada y salida del parque
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
            skCliente.close();
        }
        catch (Exception e) {
            System.out.println("Error: " + e.toString());
        }
    }
}
