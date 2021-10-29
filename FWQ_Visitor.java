import java.io.*;
import java.net.*;
import java.util.Properties;
 
import org.apache.kafka.clients.producer.*;

public class FWQ_Visitor {
	// Variables globales
	int ALIAS = 0;


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
		// Los datos se implementan mediante variables globales incrementables
		System.out.println("Datos utilizados ->" + 
			" Alias/ID: " + ALIAS +
			" Nombre: nombre" + ALIAS + 
			" Contraseña: pw" + ALIAS);
		p_Cadena = p_operacion + ";" + ALIAS + 
			";nombre" + ALIAS + ";pw" + ALIAS;
		ALIAS++;
		escribeSocket(p_Socket_Con_Servidor, p_Cadena);
		p_Cadena = "";
		p_Cadena = leeSocket(p_Socket_Con_Servidor, p_Cadena);
		p_resultado = p_Cadena;

		return p_resultado;
	}

	public String modificarDatos(String p_operacion, String p_resultado, String p_Cadena, Socket p_Socket_Con_Servidor) {
		// Problema: tener en cuenta cual era el alias anterior para mantenerlo

		//------------//
		return "";
		//------------//
	}

	public void enviarKafka(KafkaProducer producer, String topic, String key, String value) {
		// Preguntar construccion del mensage con el topic (topic, key, value)
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

		try {
			producer.send(record);
		}
		catch(Exception e) {
			System.out.println("Error: " + e.toString());
		}
	}

	public boolean consultaEntrada(String AliasVisitor, String PWVisitor, Socket skRegistro) {
		// Se comprueba si los datos estan registrados
		boolean result = false;
		String cadena = "";

		cadena = "entrar;" + AliasVisitor + ";" + PWVisitor; 

		escribeSocket(skRegistro, cadena);
		cadena = leeSocket(skRegistro, cadena);
		if (cadena == "true") {
			result = true;
		}
		else {
			result = false;
		}

		return result;
	}

	public String entrarParque(String op, String resultado, Socket skRegistro) {
		String AliasVisitor = "";
		String PWVisitor = "";
		boolean visitorExists = false;

		InputStreamReader isr = new InputStreamReader(System.in);
		BufferedReader br = new BufferedReader(isr);

		try {
		System.out.println("Introduzca su Alias/ID: ");
		AliasVisitor = br.readLine();
		System.out.println("\nIntroduzca su contraseña: ");
		PWVisitor = br.readLine();
		}
		catch (Exception e) {
			System.out.println("Error: " + e.toString());
		}

		//-----------//
		// Conexion con BD para comprobar alias y password //
		//-----------//
		visitorExists = consultaEntrada(AliasVisitor, PWVisitor, skRegistro);
		if (visitorExists) {
			// El visitante existe, hay que comprobar el aforo

		}
		else {
			// El visitante no existe, se notifica
			System.out.println("El usuario y contraseña introducidos no estan registrados");

		}

		// Despues de comprobar si el usuario existe y el aforo no esta completo
		Properties kafkaProps = new Properties();
		
		// Puede que broker2 no sea necesario
		kafkaProps.put("bootstrap.servers", "broker1:9092,broker2:9092");
		kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); 
		kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		KafkaProducer producer = new KafkaProducer<String, String>(kafkaProps);
		// Se enviara asincronamente con send()
		enviarKafka(producer, "topic", "key", "value");

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
			// Habria que quitar el socket de gestorColas, implementado mediante Kafka
			//Socket skGestorColas = new Socket(p_QueueHandlerHost, Integer.parseInt(p_QueueHandlerPort));

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
						op = "registro";
						resultado = pedirDatos(op, resultado, cadena, skRegistro);
					break;
					case 2:
						// Se modifican los datos del cliente
						op = "modificacion";
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
					String[] vectorResultados = resultado.split(" ");
					System.out.println("Los datos introducidos son ->" + 
						" Alias/ID: " + vectorResultados[0] + 
						" Nombre: " + vectorResultados[1] +
						" Contraseña: "+ vectorResultados[2]);
					// resp marca si el visitante quiere hacer alguna operacion mas
					resp = 'x';
					while (resp != 's' || resp != 'n') {
						System.out.println("¿Desea realizar alguna operacion mas? (s, n)");
						resp = br.readLine().charAt(0);
					}
					if (resp != 's') {
						salir = 1;
						escribeSocket(skRegistro, "fin");
						cadena = leeSocket(skRegistro, cadena);
						resultado = cadena;
						if (resultado == "") {
							skRegistro.close();
							System.out.println("Conexion cerrada");
							System.exit(0);
						}
					}
				}
				else if (operacion == 3 || operacion == 4) {
					// El visitante quiere entrar o salir del parque
					// TODO implementar la comunicacion con FWQ_Engine mediante Kafka
					if (operacion == 3) {
						// Se quiere entrar al parque
						entrarParque(op, resultado, skRegistro);
					}
					else if (operacion == 4) {
						// Se quiere salir del parque (no se si serviria con un System.exit(0) directo)

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
		// Implementacion de la eleccion mediante numero aleatorio (num de acciones posibles es 2)
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
}
