import java.lang.Exception;
import java.net.Socket;
import java.net.SocketException;
import java.io.*;
import java.sql.*;
import java.security.*;
import java.nio.charset.StandardCharsets;


public class FWQ_HiloServidorRegistro extends Thread {
    private static final String CONNECTIONURL = "jdbc:mysql://localhost:3306/FWQ_BBDD?useSSL=false";
    private static final String USER = "root";
    private static final String PASSWORD = "1234";

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
		catch (SocketException e) {
			System.out.println("Se ha perdido la conexion");
		}
        catch (Exception e) {
            System.out.println("Error al leer el socket");
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
			System.out.println("Error al escribir en el socket");
		}
		return;
	}

    //pasar el hash de bytes a hexadecimal
    private static String bytesToHex(byte[] hash) {
        StringBuilder hexString = new StringBuilder(2 * hash.length);
        for (int i = 0; i < hash.length; i++) {
            String hex = Integer.toHexString(0xff & hash[i]);
            if(hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }
        return hexString.toString();
    }

    //funci�n que hashea la password 20 veces
    public String hash(String password){
        byte[] hash = password.getBytes(StandardCharsets.UTF_8);

        System.out.println("Empieza el hash");

        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            //hasheamos 20 veces
            for(int i = 0; i<20; i++){
                hash = digest.digest(hash);
            }
        }
        catch (Exception e) {
            System.out.println("Error al hacer el hash");
        }

        System.out.println("Termina el hash");
        
        String hexHash = bytesToHex(hash);

        return hexHash;
    }

    // Conexion con la Base de datos

    // Insertar usuario
    public void InsertarUsuarioSQL(String Alias, String nombre, String password) {
        try {
            Connection connection = DriverManager.getConnection(CONNECTIONURL, USER, PASSWORD);
            
            password = hash(password);
            //DEBUG
            System.out.println("Password: " + password);
            //DEBUG

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
            System.out.println("Error al insertar un usuario en SQL. " + e.getMessage());
        }
    }
   public void UpdateUsuarioSQL(String Alias, String nombre, String password) {
        try {
            Connection connection = DriverManager.getConnection(CONNECTIONURL, USER, PASSWORD);
            
            password = hash(password);
            //DEBUG
            System.out.println("Password: " + password);
            //DEBUG

            Statement statement = connection.createStatement();
            String sentence = "UPDATE Usuarios SET Nombre = '" + nombre + 
                "', Contrasenya = '" + password + "' WHERE Alias = '" + Alias + "'";

            System.out.println("Actualizando usuario...");
            // Sentencia de insercion
            statement.executeUpdate(sentence);
            System.out.println("Usuario Actualizado.");
            statement.close();
        }
        catch (SQLException e) {
            System.out.println("Error al actualizar el usuario en SQL");
        }
    }

    public boolean ConsultarUsuarioSQL(String Alias, String password) {
        boolean resultado = false;
        
        try {
            Connection connection = DriverManager.getConnection(CONNECTIONURL, USER, PASSWORD);
            
            password = hash(password);
            //DEBUG
            System.out.println("Password: " + password);
            //DEBUG

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
            statement.close();
        }
        catch (SQLException e) {
            System.out.println("Error al consultar usuarios en SQL");
        }

        return resultado;
    }

    public String consultarSocket(String cadena) {
        // El caracter de separacion sera el punto y coma ';'
        // Se guardara Alias/ID, Nombre y contrasenya

        String[] operacion = cadena.split(";");
        String result = "";

        if (operacion[0].equals("registrar")) {
            // Se quiere registrar un usuario
            System.out.println("Se registrara el usuario con Alias/ID: " + operacion[1]
                + "; Nombre: " + operacion[2]
                + "; Contrasenya: " + operacion[3]);
            
            InsertarUsuarioSQL(operacion[1], operacion[2], operacion[3]);
            result = cadena;
        }
        else if (operacion[0].equals("modificar")) {
            // Se quiere modificar un usuario
            System.out.println("Se modificara el usuario para tener Alias/ID: " + operacion[1]
                + "; Nombre: " + operacion[2]
                + "; Contrasenya: " + operacion[3]);
            
            UpdateUsuarioSQL(operacion[1], operacion[2], operacion[3]);
            result = cadena;
        }
        else if (operacion[0].equals("consultar")) {
            // Se quiere saber si el usuario esta registrado
            System.out.println("Se consultara el usuario con Alias/ID: " + operacion[1]
                + "; Contrasenya: " + operacion[2]);

            boolean consulta = ConsultarUsuarioSQL(operacion[1], operacion[2]);
            result = String.valueOf(consulta);
        }

        else if (operacion[0].equals("fin")) {
            System.out.println("Se va a salir del parque");
            result = "fin";
        }
        else {
            result = "fin";
        }

        return result;
    }

    public void run() {
        String cadena = "", resultado = "";

        try {
            while (resultado != "fin") {
                cadena = this.leeSocket(skCliente, cadena);
                resultado = this.consultarSocket(cadena);
                cadena = "" + resultado;
                this.escribeSocket(skCliente, cadena);
            }
            skCliente.close();
        }
        catch (SocketException e) {
            System.out.println("Error: El visitante se ha desconectado");
        }
        catch (IOException e) {
            System.out.println("Error al manipular los sockets");
        }
    }
}
