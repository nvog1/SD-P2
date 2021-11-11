import java.lang.Exception;
import java.net.Socket;
import java.io.*;
import java.sql.*;


public class FWQ_HiloServidorRegistro extends Thread {
    private static final String CONNECTIONURL = "jdbc:mysql://localhost:3306/FWQ_BBDD?useSSL=false";
    private static final String USER = "root";
    private static final String PASSWORD = "a96556994";

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

    // Insertar usuario
    public void InsertarUsuarioSQL(String Alias, String nombre, String password) {
        try {
            Connection connection = DriverManager.getConnection(CONNECTIONURL, USER, PASSWORD);
            
            Statement statement = connection.createStatement();
            String sentence = "INSERT INTO Usuarios VALUES ('" + Alias 
                + "', '" + nombre + "', '" + password + "')";

            System.out.println("Insertando usuario...");
            // Sentencia de insercion
            statement.executeUpdate(sentence);
            System.out.println("Usuario insertado");
            statement.close();
        }
        catch (SQLException e) {
            System.out.println("Error SQL: " + e.getMessage());
        }
    }
   public void UpdateUsuarioSQL(String Alias, String nombre, String password) {
        try {
            Connection connection = DriverManager.getConnection(CONNECTIONURL, USER, PASSWORD);
            
            Statement statement = connection.createStatement();
            String sentence = "UPDATE Usuarios SET Nombre = '" + nombre + 
                "', Contrasenya = '" + password + " WHERE Alias = '" + Alias + "'";

            System.out.println("Actualizando usuario...");
            // Sentencia de insercion
            statement.executeUpdate(sentence);
            System.out.println("Usuario Actualizado.");
            statement.close();
        }
        catch (SQLException e) {
            System.out.println("Error SQL: " + e.getMessage());
        }
    }

    public boolean ConsultarUsuarioSQL(String Alias, String password) {
        boolean resultado = false;
        
        try {
            Connection connection = DriverManager.getConnection(CONNECTIONURL, USER, PASSWORD);
            
            Statement statement = connection.createStatement();
            String sentence = "SELECT Contrasenya FROM Usuarios WHERE Alias = '" + Alias + "'";
            ResultSet result = statement.executeQuery(sentence);
            if (result.next()) {
                // Algun usuario concuerda con los datos
                System.out.println("El usuario esta registrado.");
                resultado = true;
            }
            else {
                // Result esta vacio,no hay ningun usuario que concuerde
                System.out.println("El usuario no esta registrado.");
                resultado = false;
            }
        }
        catch (SQLException e) {
            System.out.println("Error SQL: " + e.getMessage());
        }

        return resultado;
    }

    public String consultarSocket(String cadena) {
        // El caracter de separacion sera el punto y coma ';'
        // Se guardara Alias/ID, Nombre y contrase�a

        String[] operacion = cadena.split(";");
        String result = "";

        /*if (operacion[0] == "entrar") {
            // Se quiere entrar al parque
            System.out.println("Se comprobaran los datos del Alias: " + operacion[1] 
                + " y constraseña: " + operacion[2]);

            try {
            //------------------//
            // Conexion con la BD //
            //------------------//

            }
            catch (Exception e) {
                System.out.println("Error: " + e.toString());
            }

        }
        else*/ if (operacion[0] == "registrar") {
            // Se quiere registrar un usuario
            System.out.println("Se registrará el usuario con Alias/ID: " + operacion[1]
                + "; Nombre: " + operacion[2]
                + "; Contraseña: " + operacion[3]);
            
            InsertarUsuarioSQL(operacion[1], operacion[2], operacion[3]);
            result = cadena;
        }
        else if (operacion[0] == "modificar") {
            // Se quiere modificar un usuario
            System.out.println("Se modificará el usuario con Alias/ID: " + operacion[1]
                + "; Nombre: " + operacion[2]
                + "; Contraseña: " + operacion[3]);
            
            UpdateUsuarioSQL(operacion[1], operacion[2], operacion[3]);
            result = cadena;
        }
        else if (operacion[0] == "consultar") {
            // Se quiere saber si el usuario esta registrado
            System.out.println("Se consultará el usuario con Alias/ID: " + operacion[1]
                + "; Contraseña: " + operacion[2]);

            ConsultarUsuarioSQL(operacion[1], operacion[2]);
            result = "true";
        }

        else if (operacion[0] == "fin") {
            System.out.println("Se va a salir del parque");
            result = "fin";
        }

        /*
        if (operacion.length == 2) {
            // Comprobacion del Alias y Password
            System.out.println("Se comprobarán los datos del Alias " + operacion[0] +
                " y contraseña " + operacion[1]);
            
            //----------//
            // Conexion con la BD //
            //----------//
            
        }
        else if (operacion.length == 4) {
            // Registro de usuario en BD
            System.out.println("SRV: se va a " + operacion[0] + " un perfil con los siguientes datos" +
            " Alias/ID: " + operacion[1] + "; Nombre: " + operacion[2] + 
            "; Contraseña: " + operacion[3]);
            
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
        }*/

        return result;
    }

    public void run() {
        String cadena = "", resultado = "";

        try {
            while (resultado != "fin") {
                cadena = this.leeSocket(skCliente, cadena);
                resultado = this.consultarSocket(cadena);
                this.escribeSocket(skCliente, cadena);
            }
            skCliente.close();
        }
        catch (Exception e) {
            System.out.println("Error: " + e.toString());
        }
    }
}
