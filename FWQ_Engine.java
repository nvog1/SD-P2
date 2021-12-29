import java.net.*;
import java.lang.Object;
import java.lang.Exception;
import java.lang.reflect.Array;
import java.net.Socket;
import java.io.*;
import java.time.Duration;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

public class FWQ_Engine {
	private static String ipWTS;
	private static Integer puertoWTS;
	// En atracciones se guarda: ID; posX; posY; tiempoEspera; tiempoCiclo
	//private static volatile String atracciones;
	
    private static final String CONNECTIONURL = "jdbc:mysql://localhost:3306/FWQ_BBDD?useSSL=false";
    private static final String USER = "root";
    private static final String PASSWORD = "1234";


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
		catch (SocketException e) {
			System.out.println("Se ha perdido la conexion");
		}
		catch (Exception e)
		{
			System.out.println("Error al leer del socket, " + e.toString());
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
			System.out.println("Error al escribir en el socket, " + e.toString());
		}
		return;
	}

	public boolean consultarAtraccion(String IdAtraccion) {
		boolean resultado = false;

		try {
			Connection connection = DriverManager.getConnection(CONNECTIONURL, USER, PASSWORD);
            Statement statement = connection.createStatement();
            String sentence = "SELECT * FROM fwq_bbdd.atracciones WHERE ID = " + IdAtraccion;
			ResultSet result = statement.executeQuery(sentence);
			if (result.next()) {
				resultado = true;
			}
			statement.close();
		}
		catch (SQLException e) {
			System.out.println("Error SQL al consultar la atraccion " + IdAtraccion + ". " + e.getMessage());
		}

		return resultado;
	}

	public void actualizarAtraccion(String ID, String posX, String posY, String tiempoEspera, String tiempoCiclo) {
		try {
			Connection connection = DriverManager.getConnection(CONNECTIONURL, USER, PASSWORD);
			PreparedStatement ps = connection.prepareStatement("UPDATE fwq_bbdd.atracciones SET posX=?, posY=?, tiempoEspera=?, tiempoCiclo=? WHERE ID=?");
			// Rellenar los parametros
			ps.setString(1, posX);
			ps.setString(2, posY);
			ps.setString(3, tiempoEspera);
			ps.setString(4, tiempoCiclo);
			ps.setString(5, ID);

			ps.executeUpdate();
			ps.close();
			/*Connection connection = DriverManager.getConnection(CONNECTIONURL, USER, PASSWORD);
            Statement statement = connection.createStatement();
            String sentence = "UPDATE fwq_bbdd.atracciones SET posX=" + posX + ", posY=" + posY + 
				", tiempoEspera=" + tiempoEspera + ", tiempoCiclo=" + tiempoCiclo + 
				" WHERE ID=" + ID;
			statement.executeUpdate(sentence);
			statement.close();*/
		}
		catch (SQLException e) {
			System.out.println("Error SQL al actualizar la atraccion " + ID);
		}
	}

	public void insertarAtraccion(String ID, String posX, String posY, String tiempoEspera, String tiempoCiclo) {
		try {
			Connection connection = DriverManager.getConnection(CONNECTIONURL, USER, PASSWORD);
            Statement statement = connection.createStatement();
            String sentence = "INSERT INTO fwq_ddbb.atracciones VALUES (" + ID + ", " + posX + 
				", " + posY + ", " + tiempoEspera + ", " + tiempoCiclo;
			statement.executeUpdate(sentence);
			statement.close();
		}
		catch (SQLException e) {
			System.out.println("Error SQL al insertar la atraccion " + ID);
		}
	}

	public void guardarAtracciones(String infoAtracciones) {
		String[] lineaAtraccion = infoAtracciones.split("\n");

		for (String linea: lineaAtraccion) {
			// linea: ID; posX; posY; tiempoEspera; tiempoCiclo
			boolean existeAtraccion = false;
			String[] datosAtraccion = linea.split(";");
			// Comprobamos si esta, si no esta se inserta, si esta se actualiza
			
			actualizarAtraccion(datosAtraccion[0], datosAtraccion[1], datosAtraccion[2], datosAtraccion[3], datosAtraccion[4]);

			/*existeAtraccion = consultarAtraccion(datosAtraccion[0]);
			if (existeAtraccion) {
				// Se actualiza la atraccion
				actualizarAtraccion(datosAtraccion[0], datosAtraccion[1], datosAtraccion[2], datosAtraccion[3], datosAtraccion[4]);
			}
			else {
				// Se inserta la atraccion
				insertarAtraccion(datosAtraccion[0], datosAtraccion[1], datosAtraccion[2], datosAtraccion[3], datosAtraccion[4]);
			}*/
		}
		System.out.println("Informacion de atracciones procesada");
	}

	//hilo que hace request al WTS para saber el estado de las atracciones
	Runnable sckRequest = new Runnable() {
		public void run() {
			String mensaje = "";
			try{
				Socket skCliente = new Socket(FWQ_Engine.ipWTS, FWQ_Engine.puertoWTS);
				mensaje = leeSocket(skCliente, mensaje);
				//FWQ_Engine.atracciones = mensaje;
				//DEBUG
				System.out.println(mensaje);
				//DEBUG
				// Info de atracciones se guarda en base de datos
				guardarAtracciones(mensaje);
			}
			catch(Exception e){
				System.out.println("Error al ejecutar sckRequest, " + e.toString());
			}

		}
	};

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		try
		{
			if (args.length < 6) {
				System.out.println("Indica: ip_broker puerto_broker ip_wts puerto_wts maxVisitantes segundos ciudad1 ciudad2 ciudad3 ciudad4");
				System.exit(1);
			}
			String ip_broker = "";
			String puerto_broker = "";
			//String ip_wts = "";
			//String puerto_wts = "";
			int maxVisitantes = -1;
			int segundos = -1; //segundos de espera entre peticiones al wts
			String ciudad1, ciudad2, ciudad3, ciudad4; 

			ip_broker = args[0];
			puerto_broker = args[1];
			FWQ_Engine.ipWTS = args[2];
			try{
				FWQ_Engine.puertoWTS = Integer.parseInt(args[3]);
				maxVisitantes = Integer.parseInt(args[4]);
				segundos = Integer.parseInt(args[5]);
			}
			catch(Exception e){
				System.out.println("Error al convertir par�metros");
			}
			ciudad1 = args[6];
			ciudad2 = args[7];
			ciudad3 = args[8];
			ciudad4 = args[9];
			

			// Hilo de kafka
			Thread tKafka = new FWQ_HiloEngineKafka(ip_broker, puerto_broker, maxVisitantes, segundos, ciudad1, ciudad2, ciudad3, ciudad4);
			tKafka.start();

			//conexion a wts
			FWQ_Engine engine = new FWQ_Engine();

			// Hilo de Sockets
			ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
			executor.scheduleAtFixedRate(engine.sckRequest, 0, segundos, TimeUnit.SECONDS);
		}
		catch(Exception e)
		{
			System.out.println("Error en el main, " + e.toString());
		}


	}
}
