import java.io.*;
import java.net.*;
import java.util.Properties;
import java.util.*;
import java.time.*;
 
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;

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

	// Se piden los datos del visitante
	public String pedirDatos(String p_operacion, String p_resultado, String p_Cadena, Socket p_Socket_Con_Servidor) {
		String Alias = "", nombre = "", contrasenya = "";
		InputStreamReader isr = new InputStreamReader(System.in);
		BufferedReader br = new BufferedReader(isr);

		try {
		System.out.println("Introduzca su Alias/ID: ");
		Alias = br.readLine();
		System.out.println("Introduzca su nombre: ");
		nombre = br.readLine();
		System.out.println("Introduzca su contrasenya: ");
		contrasenya = br.readLine();

		}
		catch (Exception e) {
			System.out.println("Error: " + e.toString());
		}

		p_Cadena = p_operacion + ";" + Alias + 
			";" + nombre + ";" + contrasenya;
		escribeSocket(p_Socket_Con_Servidor, p_Cadena);
		p_Cadena = "";
		p_Cadena = leeSocket(p_Socket_Con_Servidor, p_Cadena);
		p_resultado = p_Cadena;

		return p_resultado;
	}

	public String modificarDatos(String p_operacion, String p_resultado, String p_Cadena, Socket p_Socket_Con_Servidor) {
		String Alias = "", contrasenya = "";
		String nuevoNombre = "", nuevaContras = "";
		InputStreamReader isr = new InputStreamReader(System.in);
		BufferedReader br = new BufferedReader(isr);

		try {
			System.out.println("Introduzca su Alias/ID: ");
			Alias = br.readLine();
			System.out.println("Introduzca su contrasenya: ");
			contrasenya = br.readLine();

			// Se comprueban los datos introducidos
			p_Cadena = "consultar;" + Alias + 
				";" + contrasenya;
			escribeSocket(p_Socket_Con_Servidor, p_Cadena);
			p_Cadena = "";
			p_Cadena = leeSocket(p_Socket_Con_Servidor, p_Cadena);

			if (p_Cadena.equals("true")) {
				// Se piden los datos nuevos y modifica la BD
				System.out.println("El Alias/ID se mantendra.");
				System.out.println("Introduzca el nuevo nombre: ");
				nuevoNombre = br.readLine();
				System.out.println("Introduzca la nueva contrasenya: ");
				nuevaContras = br.readLine();

				// Conexion con la base de datos
				p_Cadena = "modificar;" + Alias + ";" +
					nuevoNombre + ";" + nuevaContras;
				escribeSocket(p_Socket_Con_Servidor, p_Cadena);
				p_Cadena = "";
				p_Cadena = leeSocket(p_Socket_Con_Servidor, p_Cadena);
				p_resultado = p_Cadena;
			}
			else {
				// Los datos introducidos no son correctos
				System.out.println("Los datos introducidos no son correctos");
				p_resultado = "-1";
			}
		}
		catch (Exception e) {
			System.out.println("Error: " + e.toString());
		}

		return p_resultado;
	}

	public void enviarKafka(KafkaProducer producer, String topic, String key, String value, String p_QueueHandlerHost, String p_QueueHandlerPort) {
		// Preguntar construccion del mensage con el topic (topic, key, value)
		ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

		try {
			// Aqui hay un warning que podemos obviar
			producer.send(record, new DemoProducerCallback());
		}
		catch(Exception e) {
			System.out.println("Error: " + e.toString());
		}

		// Consumer recibe la respuesta
		Properties ConsumerProps = new Properties();
		ConsumerProps.put("bootstrap.servers", p_QueueHandlerHost + ":" + p_QueueHandlerPort);
        ConsumerProps.put("group.id", "Visitors");
        ConsumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        ConsumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		try {
			KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(ConsumerProps);
			// Suscribir el consumer a un topic
			// TODO: En vez de "SD" poner "mapaX" (variable pasada por parametro al ejecutar la instruccion)
			consumer.subscribe(Collections.singletonList("SD"));
			Duration timeout = Duration.ofMillis(100);
			boolean continuar = true;

			while (continuar) {
				ConsumerRecords<String, String> records = consumer.poll(timeout);

				for (ConsumerRecord<String, String> consumerRecord : records) {
					if (consumerRecord.value().equals("entrar")) {
						System.out.println("El usuario puede entrar");
						continuar = false;
					}
				}
			}
		}
		catch (Exception e) {
			System.out.println("Error: " + e.toString());
		}
	}

	public String recibirKafka() {
		String result = "";


		return result;
	}

	public String entrarParque(String op, String resultado, String p_QueueHandlerHost, String p_QueueHandlerPort) {
		String AliasVisitor = "";
		String PWVisitor = "";
		boolean visitorExists = false;

		InputStreamReader isr = new InputStreamReader(System.in);
		BufferedReader br = new BufferedReader(isr);

		try {
		System.out.println("Introduzca su Alias/ID: ");
		AliasVisitor = br.readLine();
		System.out.println("Introduzca su contrasenya: ");
		PWVisitor = br.readLine();

		// Consulta si el usuario esta registrado
		Properties kafkaProps = new Properties();
		
		kafkaProps.put("bootstrap.servers", p_QueueHandlerHost + ":" + p_QueueHandlerPort);
		kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); 
		kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		KafkaProducer producer = new KafkaProducer<String, String>(kafkaProps);
		// Se enviara asincronamente con send()
		// topic = "Visitor", key = "entrarSalir", value = "entrar;Alias"
		enviarKafka(producer, "Visitor", "entrarSalir", "entrar;" + AliasVisitor, p_QueueHandlerHost, p_QueueHandlerPort);
		// topic = Alias, key = accion, value = info adicional a accion
		//enviarKafka(producer, AliasVisitor, "entrarSalir", "0", p_QueueHandlerHost, p_QueueHandlerPort);		
		}
		catch (Exception e) {
			System.out.println("Error: " + e.toString());
		}

		return resultado;
	}

	public String salirParque() {
		
	//-----------//
	return "";
	//-----------//
	}

	// El visitante podra registrarse, modificar sus datos, entrar al parque, salir del parque...
	public void pedirOperacion(String p_registryHost, String p_registryPort, String p_QueueHandlerHost, String p_QueueHandlerPort) {
		int operacion, salir = 0;
		char resp;
		String resultado = "";

		String cadena = "";
		String op = "";
		InputStreamReader isr = new InputStreamReader(System.in);
		BufferedReader br = new BufferedReader(isr);

		try {
			Socket skRegistro = new Socket(p_registryHost, Integer.parseInt(p_registryPort));

			while (salir == 0) {
				operacion = 0;
				while (operacion != 1 && operacion != 2 && operacion != 3 && operacion != 4) {
					System.out.println("[1] Registrar usuario");
					System.out.println("[2] Modificar usuario");
					System.out.println("[3] Entrar al parque");
					System.out.println("[4] Salir del parque");
					System.out.println("Elige la opcion que desea realizar:");
					operacion = Integer.parseInt(br.readLine());
					System.out.println("");
				}
				
				switch(operacion) {
					case 1:
						// Se registra al visitante
						op = "registrar";
						resultado = pedirDatos(op, resultado, cadena, skRegistro);
					break;
					case 2:
						// Se modifican los datos del cliente
						op = "modificar";
						resultado = modificarDatos(op, resultado, cadena, skRegistro);
					break;
					case 3: 
						// Se quiere entrar al parque
						op = "entrar";
					break;
					case 4:
						// Se quiere salir del parque
						op = "salir";
					break;
				}
				if (operacion == 1 || operacion == 2) {
					// Se realiza una operacion de registro o modificacion de datos
					String[] vectorResultados = resultado.split(";");
					System.out.println("Los datos introducidos son ->" + 
						" Alias/ID: " + vectorResultados[1] + 
						" Nombre: " + vectorResultados[2] +
						" Contrasenya: "+ vectorResultados[3]);
					// resp marca si el visitante quiere hacer alguna operacion mas
					resp = 'x';
					while (resp != 's' && resp != 'n') {
						System.out.println("¿Desea realizar alguna operacion mas? (s, n)");
						resp = br.readLine().charAt(0);
					}
					if (resp != 's') {
						salir = 1;
						escribeSocket(skRegistro, "fin");
						cadena = leeSocket(skRegistro, cadena);
						resultado = cadena;
						if (resultado == "fin") {
							skRegistro.close();
							System.out.println("Conexion cerrada");
							System.exit(0);
						}
					}
				}
				else if (operacion == 3 || operacion == 4) {
					// El visitante quiere entrar o salir del parque
					if (operacion == 3) {
						// Se quiere entrar al parque
						entrarParque(op, resultado, p_QueueHandlerHost, p_QueueHandlerPort);
						/*System.out.println("Se va a crear un topic...");
						KafkaTopic topic = new KafkaTopic(p_QueueHandlerHost, p_QueueHandlerPort, "TopicPrueba", "Visitor");
						salir = 1;*/
					}
					else if (operacion == 4) {
						escribeSocket(skRegistro, "fin");
						cadena = leeSocket(skRegistro, cadena);
						if (cadena == "fin") {
							skRegistro.close();
							System.out.println("Conexion cerrada");
							System.exit(0);
						}
						System.out.println("Saliendo del parque...");
						salir = 1;
					}
				}
				cadena = "";
				op = "";
			}
		}
		catch (Exception e) {
			System.out.println("Error: " + e.toString());
		}
	}

	// El visitante podra hacer alguna actividad relacionada con el parque o salir
	public void menu(String p_registryHost, String p_registryPort, String p_QueueHandlerHost, String p_QueueHandlerPort) {
		int opcion = 0;

		try {
			while (opcion != 1 && opcion != 2) {
				System.out.println("[1] Realizar operacion referente al parque");
				System.out.println("[2] Salir");
				InputStreamReader isr = new InputStreamReader(System.in);
				BufferedReader br = new BufferedReader(isr);
				opcion = Integer.parseInt(br.readLine());
				System.out.println("");
			}
			if (opcion == 1) {
				// Se realiza alguna operacion
				pedirOperacion(p_registryHost, p_registryPort, p_QueueHandlerHost, p_QueueHandlerPort);
			}
			else {
				System.exit(0);
			}
		}
		catch(Exception e) {
			System.out.println("Error: " + e.toString());
		}
	}

    public static void main(String[] args) {
		// localhost 9999 localhost 
        FWQ_Visitor visitante = new FWQ_Visitor();
		int i = 0;
        String RegistryHost;
        String RegistryPort;
        String QueueHandlerHost;
        String QueueHandlerPort;
		
        if (args.length < 4) {
            System.out.println("Se debe indicar la direccion y el puerto del registro" + 
            " y del gestor de colas");
            System.out.println("$./FWQ_Visitor host_registro puerto_registro " + 
			"host_gestorColas puerto_gestorColas");
			System.exit(-1);
        }
		RegistryHost = args[0];
		RegistryPort = args[1];
		QueueHandlerHost = args[2];
		QueueHandlerPort = args[3];

		while(i == 0) {
			visitante.menu(RegistryHost, RegistryPort, QueueHandlerHost, QueueHandlerPort);
		}
    }

	private static class DemoProducerCallback implements Callback {
		@Override
		public void onCompletion(RecordMetadata recordMetadata, Exception e){
			if (e != null) {
				e.printStackTrace();
			}
		}
	}
}

