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
    private static final String CONNECTIONURL = "jdbc:mysql://localhost:3306/fwq_bbdd?useSSL=false";
    private static final String USER = "root";
    private static final String PASSWORD = "1234";
	private static String ipWTS;
	private static Integer puertoWTS;


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

	// Funcion auxiliar para calcular la nueva posicion del visitante
	public String calculaPos(String posicion, String direccion) {
		String resultado = "";
		int x, y;

		// posicion = PosX;PosY
		String[] vectorResultados = posicion.split(";");
		x = Integer.parseInt(vectorResultados[0]);
		y = Integer.parseInt(vectorResultados[1]);

		// El mapa es de 20x20
		switch(direccion) {
			case "1":
				// Norte
				y = y - 1;
				break;
			case "2":
				// Noreste
				y = y - 1;
				x = x + 1;
				break;
			case "3":
				// Este
				x = x +1;
				break;
			case "4":
				// Sureste
				x = x + 1;
				y = y + 1;
				break;
			case "5":
				// Sur
				y = y + 1;
				break;
			case "6":
				//Suroeste
				x = x - 1;
				y = y + 1;
				break;
			case "7":
				//Oeste
				x = x - 1;
				break;
			case "8":
				// Noroeste
				x = x - 1;
				y= y - 1;
				break;
		}

		if (x == -1) {
			x = 19;
		}
		if (x == 20) {
			x = 0;
		}
		if (y == -1) {
			y = 19;
		}
		if (y == 20) {
			y = 0;
		}

		resultado = x + ";" + y;
		return resultado;
	}

	// Actualiza el mapa de la base de datos
	public void actualizarMapaBD(String Alias, String posicion) {
		System.out.println("Actualizando BD...");

		try {
			String[] vectorResultados = posicion.split(";");

			Connection connection = DriverManager.getConnection(CONNECTIONURL, USER, PASSWORD);
            
            Statement statement = connection.createStatement();
            String sentence = "UPDATE Mapa SET PosX = " + Integer.parseInt(vectorResultados[0] + 
				", PosY = " + Integer.parseInt(vectorResultados[1]) + " WHERE Alias = '" + Alias + "'");

			statement.executeUpdate(sentence);
			statement.close();
			System.out.println("BD actualizada");
		}
		catch (Exception e) {
			System.out.println("Error: " + e.toString());
		}
	}

	public String CadenaMapa(Map<String, String> mapa) {
		char caracter = 'a';
		String cadena = "";
		Set<String> AliasMapa = mapa.keySet();
		// Matriz que representa el mapa [x][y]
		String[][] matriz = new String[20][20];

		
		cadena = "Leyenda del mapa\n" + 
			"Caracter\tID\tPos\n";

		// Recorre todo el map para hacer la leyenda
		for (String Alias : AliasMapa) {
			String[] vectorResultados = mapa.get(Alias).split(";");
			int x = Integer.parseInt(vectorResultados[0]);
			int y = Integer.parseInt(vectorResultados[1]);
			cadena = cadena + caracter + "\t" + Alias + "\t" + mapa.get(Alias) + "\n";

			matriz[y][x] = Character.toString(caracter);
			caracter++;
		}

		cadena = cadena + "MAPA DEL PARQUE\n";

		// Creacion del mapa
		for(int i = 0; i < 20; i++) {
			for (int j = 0; j < 20; j++){
				if (matriz[i][j] == null) {
					cadena = cadena + "·";
				}
				else {
					// Implementado para los visitantes
					Character aux = matriz[i][j].charAt(0);
					if (aux.isLetter(matriz[i][j].charAt(0))) {
						// En la matriz hay un caracter (un visitante)
						cadena = cadena + matriz[i][j];
					}
				}
			}
			cadena = cadena + "\n";
		}

		return cadena;
	}

	// Actualiza y devuelve el mapa
	public Map<String, String> actualizarMapa(String Alias, String direccion) {
		// Query del mapa, recalcular posicion del alias y actualizar el mapa
		Map<String, String> resultado = new HashMap<String, String>();

		try {
			Connection connection =  DriverManager.getConnection(CONNECTIONURL, USER, PASSWORD);
			Statement statement = connection.createStatement();
			ResultSet result = statement.executeQuery("SELECT * from FWQ_BBDD.Mapa");
			// Se procesan los resultados obtenidos y modifica el alias
			while (result.next()) {
				String AliasMap = result.getString("Alias");
				String posMap = result.getInt("PosX") + ";" + result.getInt("PosY");
				
				if (AliasMap == Alias) {
					// Es el usuario que queremos actualizar
					posMap = calculaPos(posMap, direccion);
				
					actualizarMapaBD(AliasMap, posMap);
				}

				resultado.put(AliasMap, posMap);
			}
		}
		catch (Exception e) {
			System.out.println("Error: " + e.toString());
		}

		return resultado;
	}

	//hilo que hace request al WTS para saber el estado de las atracciones
	Runnable sckRequest = new Runnable() {
		public void run() {
			String mensaje = "";
			try{
				Socket skCliente = new Socket(FWQ_Engine.ipWTS, FWQ_Engine.puertoWTS);
				mensaje = leeSocket(skCliente, mensaje);
			}
			catch(Exception e){
				System.out.println("Error: " + e.toString());
			}

			//DEBUG
			System.out.println(mensaje);
			//DEBUG
			
		}
	};

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
			String ip_broker = "";
			String puerto_broker = "";
			//String ip_wts = "";
			//String puerto_wts = "";
			int maxVisitantes = -1;
			int segundos = -1; //segundos de espera entre peticiones al wts

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
			

			// Hilo de kafka
			Thread tKafka = new FWQ_HiloEngineKafka(ip_broker, puerto_broker, maxVisitantes, segundos);
			tKafka.start();

			//conexion a wts
			FWQ_Engine engine = new FWQ_Engine();

			// Hilo de Sockets
			ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
			executor.scheduleAtFixedRate(engine.sckRequest, 0, segundos, TimeUnit.SECONDS);
		}
		catch(Exception e)
		{
			System.out.println("Error: " + e.toString());
		}


	}
}
