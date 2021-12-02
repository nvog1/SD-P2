import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;
import java.lang.Exception;
import java.io.*;
import java.util.Properties;
import java.util.*;
import java.time.Duration;
import java.sql.*;

public class FWQ_HiloEngineKafka extends Thread {
    private int maxVisitantes;

    private static final String CONNECTIONURL = "jdbc:mysql://localhost:3306/FWQ_BBDD?useSSL=false";
    private static final String USER = "root";
    private static final String PASSWORD = "1234";

    private Properties ProducerProps = new Properties();
    private Properties ConsumerProps = new Properties();
    private KafkaProducer<String, String> producer;
    private KafkaConsumer<String, String> consumer;


    public FWQ_HiloEngineKafka(String ipBroker, String puertoBroker, int aforo) {
        System.out.println("Configurando propiedades locales");
		maxVisitantes = aforo;

        this.ProducerProps.put("bootstrap.servers", ipBroker + ":" + puertoBroker);
        this.ProducerProps.put("key.serializer" , "org.apache.kafka.common.serialization.StringSerializer");
        this.ProducerProps.put("value.serializer" , "org.apache.kafka.common.serialization.StringSerializer");

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
		}
		catch (Exception e) {
			System.out.println("Error: " + e.toString());
		}

        return resultado;
    }

    // Comprueba si el Alias/ID esta registrado
    public boolean entrarSalir(String topic, String value) {
        Boolean op1 = false, op2 = false;
        Boolean result = false;
        String[] vectorResultados = value.split(";");

        if (vectorResultados[0] == "entrar") {
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
        }

        result = (op1 && op2);
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
    
    public void procesarKafka(String topic, String key, String value) {
        // Topic muestra el ALias/ID del Visitor
        // Key muestra la accion que se quiere hacer
        // Value muestra la opcion a la accion que se quiere hacer; el Alias del visitor; el topic de vuelta
        String[] vectorResultados = value.split(";");

        System.out.println("Topic: " + topic + "; Key: " + key + "; Value: " + vectorResultados[0]);
        if (key == "entrarSalir") {
            // Se quiere entrar (value == "0") o salir (value == "1")
            if (entrarSalir(topic, value)) {
                // El usuario esta registrado y cabe en el parque (no supera aforo)
                ProducerRecord<String, String> record = new ProducerRecord<String, String>(vectorResultados[2], key, "entrar");

                try {
                    // Aqui hay un warning que podemos obviar
                    producer.send(record/*, new DemoProducerCallback()*/);
                }
                catch(Exception e) {
                    System.out.println("Error: " + e.toString());
                }
            }
        }
    }

    public void run() {
        boolean continuar = true;
        Duration timeout = Duration.ofMillis(100);
        String topic = "", key = "", value = "";

        try {
            // Bucle de escucha kafka
            while (continuar) {
                ConsumerRecords<String, String> records = consumer.poll(timeout);

                for (ConsumerRecord<String, String> record : records) {
					System.out.println("Entro al bucle de records");
                    System.out.println("Se asignaran las variables recibidas por kafka");
                    // Asignamos las variables
                    topic = record.topic();
                    key = record.key();
                    value = record.value();

                    procesarKafka(topic, key, value);
                }
            }
        }
        catch (Exception e) {
            System.out.println("Error: " + e.toString());
        }
    }

    
	/*private static class DemoProducerCallback implements Callback {
		@Override
		public void onCompletion(RecordMetadata recordMetadata, Exception e){
			if (e != null) {
				e.printStackTrace();
			}
		}
	}*/
}