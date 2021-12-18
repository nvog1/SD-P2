import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;
import java.lang.Exception;
import java.io.*;
import java.util.Properties;
import java.util.*;
import java.time.Duration;
import java.sql.*;

public class FWQ_HiloEngineKafka extends Thread {
    private Integer maxVisitantes, tiempoSeg;
	private String mapaParque = "";

    private static final String CONNECTIONURL = "jdbc:mysql://localhost:3306/FWQ_BBDD?useSSL=false";
    private static final String USER = "root";
    private static final String PASSWORD = "1234";

    private Properties ProducerProps = new Properties();
    private Properties ConsumerProps = new Properties();
    private KafkaProducer<String, String> producer;
    private KafkaConsumer<String, String> consumer;


    public FWQ_HiloEngineKafka(String ipBroker, String puertoBroker, Integer aforo, Integer segundos) {
        System.out.println("Configurando propiedades locales");
		maxVisitantes = aforo;
		tiempoSeg = segundos;

        this.ProducerProps.put("bootstrap.servers", ipBroker + ":" + puertoBroker);
        this.ProducerProps.put("key.serializer" , "org.apache.kafka.common.serialization.StringSerializer");
        this.ProducerProps.put("value.serializer" , "org.apache.kafka.common.serialization.StringSerializer");
		this.ProducerProps.put("max.block.ms", "1000");
		this.ProducerProps.put("delivery.timeout.ms", "1900");
		this.ProducerProps.put("linger.ms", "0");
		this.ProducerProps.put("request.timeout.ms", "50");

        producer = new KafkaProducer<String, String>(ProducerProps);

        this.ConsumerProps.put("bootstrap.servers", ipBroker + ":" + puertoBroker);
        this.ConsumerProps.put("group.id", "Visitor");
        this.ConsumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        this.ConsumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaConsumer<String, String>(ConsumerProps);
        // Suscribir el consumer a un topic
		consumer.subscribe(Collections.singletonList("Visitor"));
    }
    
    public boolean ConsultarUsuarioSQL(String Alias) {
        boolean resultado = false;
        
        try {
            Connection connection = DriverManager.getConnection(CONNECTIONURL, USER, PASSWORD);
            
            Statement statement = connection.createStatement();
            String sentence = "SELECT * FROM Usuarios WHERE Alias = '" + Alias + "'";
            ResultSet result = statement.executeQuery(sentence);
            if (result.next()) {
                // Algun usuario concuerda con los datos
                resultado = true;
            }
            else {
                // Result esta vacio,no hay ningun usuario que concuerde
                resultado = false;
            }
            statement.close();
        }
        catch (SQLException e) {
            System.out.println("Error SQL: " + e.getMessage());
        }

        return resultado;
    }

    public Integer consultarNumUsuSQL() {
        Integer resultado = 0;

        try {
			Connection connection =  DriverManager.getConnection(CONNECTIONURL, USER, PASSWORD);
			Statement statement = connection.createStatement();
			ResultSet result = statement.executeQuery("SELECT * from FWQ_BBDD.Mapa");
			// Se procesan los resultados obtenidos y modifica el alias
			while (result.next()) {
				resultado++;
			}
			statement.close();
		}
		catch (Exception e) {
			System.out.println("Error: " + e.toString());
		}

        return resultado;
    }

	public Boolean ConsultarMapaSQL(String AliasVisitor) {
		Boolean boolResult = false;

		try {
			Connection connection =  DriverManager.getConnection(CONNECTIONURL, USER, PASSWORD);
			Statement statement = connection.createStatement();
			ResultSet result = statement.executeQuery("SELECT * from FWQ_BBDD.Mapa WHERE Alias='" + AliasVisitor + "'");
			// Se procesan los resultados obtenidos y modifica el alias
			if (result.next()) {
				boolResult = true;
			}
			statement.close();
		}
		catch (SQLException e) {
			System.out.println("Error al consultar el mapa SQL");
		}

		return boolResult;
	}

	public void eliminarMapaSQL(String Alias) {
		try {
			Connection connection = DriverManager.getConnection(CONNECTIONURL, USER, PASSWORD);
			Statement statement = connection.createStatement();
			String sentence = "DELETE FROM fwq_bbdd.mapa WHERE Alias='" + Alias + "'";
			statement.executeUpdate(sentence);
			statement.close();
		}
		catch (SQLException e) {
			System.out.println("Error al eliminar el visitante de mapa SQL");
		}
	}

    // Comprueba si el Alias/ID esta registrado
    public boolean entrar(String topic, String value) {
        Boolean op1 = false, op2 = false;
        Boolean result = false;
        String[] vectorResultados = value.split(";");

        if (vectorResultados[0].equals("entrar")) {
            // El usuario quiere entrar al parque
            System.out.println("El usuario " + vectorResultados[1] + " quiere entrar al parque");

            // Comprobacion de que el usuario esta registrado
            if (ConsultarUsuarioSQL(vectorResultados[1])) {
                System.out.println("El usuario esta registrado.");
                op1 = true;
            }
            else {
                System.out.println("El usuario no esta registrado.");
                op1 = false;
            }

			//NICO: si el usuario no está registrado, da igual si cabe o no. yo pondría este if else dentro del if está registrado
            if (consultarNumUsuSQL() > maxVisitantes) {
                // Se ha alcanzado el numero maximo de visitantes
                System.out.println("Se ha alcanzado el aforo maximo");
                op2 = false;
            }
            else {
                // Cabe mas gente
                System.out.println("El aforo del parque acepta al visitante");
                op2 = true;
            }
			result = (op1 && op2);
        }

        return result;
    }

	public Boolean salir(String topic, String value) {
        Boolean result = false;
        String[] vectorResultados = value.split(";");

		if (vectorResultados[0].equals("salir")) {
			// El usuario quiere salir del parque

			System.out.println("El usuario " + vectorResultados[1] + " quiere salir del parque");
			result = ConsultarMapaSQL(vectorResultados[1]);
			if (result) {
				// Se elimina del mapa
				eliminarMapaSQL(vectorResultados[1]);
				System.out.println("El usuario se ha eliminado del mapa");
			}
		}

		return result;
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
		System.out.println("Nueva posicion calculada: " + resultado);
		return resultado;
	}

	// Actualiza el mapa de la base de datos
	public void actualizarMapaBD(String Alias, String posicion) {
		System.out.println("Actualizando BD...");

		try {
			String[] vectorResultados = posicion.split(";");

			Connection connection = DriverManager.getConnection(CONNECTIONURL, USER, PASSWORD);
            
            Statement statement = connection.createStatement();
            String sentence = "UPDATE Mapa SET PosX = " + Integer.parseInt(vectorResultados[0]) + 
				", PosY = " + Integer.parseInt(vectorResultados[1]) + " WHERE Alias = '" + Alias + "'";

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
		String atracciones = "";
		Random rd = new Random();
		
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
		// cadena de varias lineas: ID; posX; posY; tiempoEspera; tiempoCiclo
		/*atracciones = FWQ_Engine.getAtracciones();
		String[] lineaAtracciones = atracciones.split("\n");
		Boolean boolAtraccion = false;*/
		// Creacion del mapa
		for(Integer i = 0; i < 20; i++) {
			// Para cada Y del eje
			for (Integer j = 0; j < 20; j++){
				// Para cada X del eje
				/*for (int k = 0; k < lineaAtracciones.length && !boolAtraccion; k++) {
					// Comprueba si en la posicion xy hay una atraccion
					String[] datosAtraccion = lineaAtracciones[k].split(";");
					if (j.equals(Integer.parseInt(datosAtraccion[1])) && i.equals(Integer.parseInt(datosAtraccion[2]))) {
						boolAtraccion = true;
						// Se añade el tiempo de espera al mapa
						cadena = cadena + datosAtraccion[3];
					}
				}*/
				if (j.equals(5) && i.equals(2)) {
					cadena = cadena + rd.nextInt(79)+1;
				}
				else if (j.equals(10) && i.equals(3)) {
					cadena = cadena + rd.nextInt(79)+1;
				}
				else if (j.equals(7) && i.equals(2)) {
					cadena = cadena + rd.nextInt(79)+1;
				}
				else if (matriz[i][j] == null) {
					cadena = cadena + " . ";
				}
				else {
					// Implementado para los visitantes
					Character aux = matriz[i][j].charAt(0);
					//if (aux.isLetter(matriz[i][j].charAt(0))) {
					if (aux >= 97 && aux <= 122) {
						// En la matriz hay un caracter (un visitante)
						cadena = cadena + " " + matriz[i][j] + " ";
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
				
				if (AliasMap.equals(Alias)) {
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

	public void comprobarMapa(String AliasVisitor, String posX, String posY) {
		try {
			// Se comprueba si el usuario esta dentro del parque, si no lo esta se inserta a fwq_bbdd
			Connection connection =  DriverManager.getConnection(CONNECTIONURL, USER, PASSWORD);
			Statement statement = connection.createStatement();
			ResultSet result = statement.executeQuery("SELECT * from FWQ_BBDD.Mapa WHERE Alias='" + AliasVisitor + "'");
			if (result.next()) {
				// Existe el usuario en la tabla mapa, no se hace nada
			}
			else {
				// No existe el usuario, se inserta
				String sentence = "INSERT INTO mapa VALUES ('" + AliasVisitor 
					+ "', '" + posX + "', '" + posY + "')";
				statement.executeUpdate(sentence);
				System.out.println("Usuario insertado en el mapa");
				statement.close();
			}
		}
		catch (SQLException e) {
			System.out.println("Error al manipular la tabla mapa SQL");
		}
	}

	public String obtenerAtracciones() {
		String cadena = "";
		
		try {
			Connection connection = DriverManager.getConnection(CONNECTIONURL, USER, PASSWORD);
            Statement statement = connection.createStatement();
            String sentence = "SELECT * FROM fwq_atracciones";
			ResultSet result = statement.executeQuery(sentence);
			while (result.next()) {
				cadena = cadena + result.getInt("ID") + "; " + result.getInt("posX") + ";" + 
					result.getInt("posY") + ";" + result.getInt("tiempoEspera") + ";" + result.getInt("tiempoCiclo") + "\n";
			}
			statement.close();
		}
		catch (SQLException e) {
			System.out.println("Error SQL al obtener las atracciones");
		}

		return cadena;
	}
    
    public void procesarKafka(String topic, String key, String value) {
        // Topic muestra el ALias/ID del Visitor
        // Key muestra la accion que se quiere hacer
        // Value muestra la opcion a la accion que se quiere hacer; el Alias del visitor; el topic de vuelta
        String[] vectorResultados = value.split(";");

        System.out.println("Topic: " + topic + "; Key: " + key + "; Value: " + vectorResultados[0]);
        if (key.equals("entrar")) {
            // Se quiere entrar 
			Boolean boolResult = entrar(topic, value);
            if (boolResult) {
                // El usuario esta registrado y cabe en el parque (no supera aforo), puede entrar
                enviarKafka(vectorResultados[2], key, "entrar");
            }
        }
		else if (key.equals("salir")) {
			// Se quiere salir
			Boolean boolResult = salir(topic, value);
            if (boolResult) {
                // El usuario estaba en el mapa, se ha eliminado
                enviarKafka(vectorResultados[2], key, "salir");
            }
		}
		else if (key.equals("Mov")) {
			// Se procesa el movimiento del visitor
			// Topic: Visitor; Key: "Mov"; Value: AliasVisitor;posX;posY;proxMov(numero);TopicConsumer
			String resultado = "";
			comprobarMapa(vectorResultados[0], vectorResultados[1], vectorResultados[2]);
			mapaParque = CadenaMapa(actualizarMapa(vectorResultados[0], vectorResultados[3]));
			// Se devuelve el mapa al visitor
			enviarKafka(vectorResultados[4], key, mapaParque);
		}
		else if (key.equals("Seg")) {
			// Se le envia al visitor el tiempo especificado
			// Topic: Visitor; Key: "Seg"; Value: AliasVisitor;TopicConsumer
			enviarKafka(vectorResultados[1], key, tiempoSeg.toString());
		}
		else if (key.equals("Atracciones")) {
			String atracciones = obtenerAtracciones();
			enviarKafka(vectorResultados[1], key, atracciones);
		}
    }

	public void enviarKafka(String topic, String key, String value) {
		ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
		System.out.println("\n----------------------------------\n" + 
			"Se va a enviar el mensaje de Kafka");
		try {
			producer.send(record);
			System.out.println("Mensaje enviado");
		}
		catch(Exception e) {
			System.out.println("Error al enviar el mensaje por Kafka. " + e.getMessage());
		}
	}

    public void run() {
        boolean continuar = true;
        Duration timeout = Duration.ofMillis(100);
        String topic = "", key = "", value = "";

        // Bucle de escucha kafka
       while (continuar) {
            ConsumerRecords<String, String> records = consumer.poll(timeout);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("Se asignaran las variables recibidas por kafka");
                // Asignamos las variables
                topic = record.topic();
                key = record.key();
                value = record.value();
                procesarKafka(topic, key, value);
            }
        }
    }
}