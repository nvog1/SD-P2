import java.io.*;
import java.net.*;
import java.util.Properties;
import java.util.*;
import java.time.*;
import java.net.ServerSocket;
import javax.net.ssl.*;
 
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;

public class FWQ_Visitor {
	private static String topicConsumer;
	private String posicionActual = "";
	// atraccionObjetivo: ID; posX; posY; tiempoEspera; TiempoCiclo
	private String atraccionObjetivo = "";
	long tiempoEspera = 0;
	private static Properties ProducerProps = new Properties();
	private static Properties ConsumerProps = new Properties();
	private static KafkaProducer producer;
	private static KafkaConsumer consumer;

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
			System.out.println("Error al leer el socket");
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
		catch (SocketException e) {
			System.out.println("Se ha perdido la conexion");
		}
		catch (Exception e)
		{
			System.out.println("Error al escribir en el socket");
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
			System.out.println("Error al introducir los datos por consola");
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
			System.out.println("Error al introucir los datos por consola");
		}

		return p_resultado;
	}

	public void seleccionarAtraccion(String cadenaAtracciones) {
		String[] lineaAtraccion = cadenaAtracciones.split("\n");
		Integer numRandom = -1;
		Random rd = new Random();
		boolean continuar = true;

		do {
			numRandom = rd.nextInt(lineaAtraccion.length);
			String[] datosAtraccion = lineaAtraccion[numRandom].split(";");
			if (Integer.parseInt(datosAtraccion[3]) < 60) {
				// Se cumple la condicion de atraccionObjetivo
				atraccionObjetivo = lineaAtraccion[numRandom];
				continuar = false;
			}
		} while(continuar);

	}

	public Integer contAbajo(int origen, int destino) {
		int contador = 0;
		int origenAux = origen;

		while (origenAux != destino) {
			contador++;
			origenAux--;
			if (origenAux < 0) {
				// Pasa de 0 a 19
				origenAux = 19;
			}
		}

		return contador * -1;
	}

	public Integer contArriba(int origen, int destino) {
		int contador = 0;
		int origenAux = origen;

		while (origenAux != destino) {
			contador++;
			origenAux++;
			if (origenAux > 19) {
				// Pasa de 19 a 0
				origenAux = 0;
			}
		}

		return contador;
	}

	public String movToAtraccion() {
		// Se movera de la posicion actual a la posicion de atraccion objetivo
		//posicionActual(posX;posY) y atraccionObjetivo(posX;posY)
		int xCircular, x, yCircular, y;
		int atraccionX, atraccionY, visitorX, visitorY;
		int movX, movY;
		String result = "";

		String[] atraccion = atraccionObjetivo.split(";");
		String[] visitor = posicionActual.split(";");

		atraccionX = Integer.parseInt(atraccion[1]);
		atraccionY = Integer.parseInt(atraccion[2]);
		visitorX = Integer.parseInt(visitor[0]);
		visitorY = Integer.parseInt(visitor[1]);

		xCircular = /*-1 * (visitorX + (19 - atraccionX) + 1)*/contAbajo(visitorX, atraccionX);
		yCircular = /*-1 * (visitorY + (19 - atraccionY) + 1)*/contAbajo(visitorY, atraccionY);
		x = /*abs(atraccionX - visitorX)*/contArriba(visitorX, atraccionX);
		y = /*abs(atraccionY - atraccionX)*/contArriba(visitorY, atraccionY);

		// Cogemos el menor componente de cada eje, diferenciamos usando negativos
		if (x <= Math.abs(xCircular)) {
			movX = x;
		}
		else {
			movX = xCircular;
		}
		if (y <= Math.abs(yCircular)) {
			movY = y;
		}
		else {
			movY = yCircular;
		}

		if (movX < 0) {
			// Se ira hacia el Oeste
			if (movY < 0) {
				// Noroeste
				result = "8";
			}
			else if (movY > 0) {
				// Suroeste
				result = "6";
			}
			else {
				// Oeste
				result = "7";
			}
		}
		else if (movX > 0) {
			// Se ira hacia el Este
			if (movY < 0) {
				// Noreste
				result = "2";
			}
			else if (movY > 0) {
				// Sureste
				result = "4";
			}
			else {
				// Este
				result = "3";
			}
		}
		else {
			// movX = 0, va hacia Norte o Sur
			if (movY < 0) {
				// Norte
				result = "1";
			}
			else if (movY > 0) {
				// Sur
				result = "5";
			}
			else {
				// movX = 0; movY = 0; destino
				result = "0";
			}
		}

		return result;
	}

	public String proximoMov() {
		String result = "";
		String topic, key, value;
		String kafkaResult = "";
		/*Random rd = new Random();

		Integer num = rd.nextInt(8) + 1;
		// Norte = 1; Noreste = 2; Este = 3; Sureste = 4; 
		// Sur = 5; Suroeste = 6; Oeste = 7; Noroeste = 8
		result = num.toString();*/

		// Para recibir las temperaturas
		topic = "Visitor";
		key = "Temperaturas";
		value = "Cadena;" + topicConsumer;
		enviarKafka(topic, key, value);
		kafkaResult = recibirKafka();
		// infoTemp = temp1;temp2;temp3;temp4 (tempX=exntremo/no)
		String[] infoTemp = kafkaResult.split(";");

		topic = "Visitor";
		key = "Atracciones";
		value = "Cadena;" + topicConsumer;kafkaResult = "";
		String atraccionAux = "";

		enviarKafka(topic, key, value);
		// kafkaResult tiene una lista de atracciones almacenadas como String:
		// ID;posX;posY;tiempoEspera;tiempoCiclo
		kafkaResult = recibirKafka();
		// Se comprueba si la atraccionActual sigue cumpliendo que tiempoEspera < 60
		String[] lineaAtraccion = kafkaResult.split("\n");

		if (!atraccionObjetivo.equals("")) {
			// Ya tiene una atraccionObjetivo
			String[] datosAtraccion = atraccionObjetivo.split(";");
			for (String linea: lineaAtraccion) {
				String[] vectorResultados = linea.split(";");
				if (vectorResultados[0].equals(datosAtraccion[0])) {
					// Se ha encontrado la linea de la atraccion, se comprueba si sigue tiempoEspera < 60
					if (Integer.parseInt(vectorResultados[3]) < 60) {
						// Sigue cumpliendo la condicion, sigue dirigiendose a esa atraccion
					}
					else {
						// Se selecciona una nueva atraccionObjetivo
						seleccionarAtraccion(kafkaResult);
					}
				}
			}
		}
		else {
			// Se selecciona una nueva atraccionObjetivo
			seleccionarAtraccion(kafkaResult);
		}

		// AtraccionObjetivo seleccionada, movimiento hacia ella
		result = movToAtraccion();

		return result;
	}

	public void dentroParque(String AliasVisitor) {
		char resp = '0';
		String mov = "";
		Random rd = new Random();

		// El visitor acaba de entrar al parque (no tiene posicion), se aplicara la logica
		if (posicionActual.equals("")) {
			// Se elige una posicion aleatoria
			int posX = rd.nextInt(20);
			int posY = rd.nextInt(20);
			posicionActual = posX + ";" + posY;
			System.out.println("Posicion del cliente: " + posicionActual);
		}
		// Ya tiene una posicion asignada
		// bucle, mandar nuevaPos, recibir mapa, Thread.sleep(numSegundos)
		while (/*resp != 's'*/true) {
			String kafkaResult = "";

			mov = proximoMov();
			String topic = "Visitor", key = "Mov", value = AliasVisitor + ";" + posicionActual + ";" + mov + ";" + topicConsumer;
			enviarKafka(topic, key, value);
			kafkaResult = recibirKafka();
			// Devuelve el mapa del parque con los otros visitantes y las atracciones
			System.out.println(kafkaResult);

			// Se para la ejecucion los segundos especificados
			try {
				Thread.sleep(tiempoEspera);
				if (mov.equals("0")) {
					// Esta en el destino
					String[] datosAtraccion = atraccionObjetivo.split(";");
					Thread.sleep(Integer.parseInt(datosAtraccion[4]));
				}
			}
			catch(Exception e) {
				System.out.println("Error durante el tiempo de espera");
			}
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

	public String recibirKafka() {
		String result = "", topic = "", key = "", value = "";
        Duration timeout = Duration.ofMillis(100);
		boolean continuar = true;

		try {
            // Bucle de escucha kafka
            while (continuar) {
                ConsumerRecords<String, String> records = consumer.poll(timeout);

				// Topic: topic de vuelta (TopicConsumer)
				// Key: Accion a realizar
				// Value: resultado de la accion
                for (ConsumerRecord<String, String> record : records) {
                    // Asignamos las variables
                    topic = record.topic();
                    key = record.key();
                    value = record.value();
					
					return value;
                }
            }
        }
        catch (Exception e) {
            System.out.println("Error al leer mensajes de Kafka");
        }

		return result;
	}

	public void entrarParque() {
		String AliasVisitor = "";
		String PWVisitor = "";
		String kafkaResult = "";
		String topic = "", key = "", value = "";
		boolean visitorExists = false;
		InputStreamReader isr = new InputStreamReader(System.in);
		BufferedReader br = new BufferedReader(isr);

		try {
			System.out.println("Introduzca su Alias/ID: ");
			AliasVisitor = br.readLine();
			System.out.println("Introduzca su contrasenya: ");
			PWVisitor = br.readLine();
			
			// Segundos especificados en argumentos al ejecutar
			if (tiempoEspera == 0) {
				topic = "Visitor";
				key = "Seg";
				value = AliasVisitor + ";" + topicConsumer;
				enviarKafka(topic, key, value);
				kafkaResult = recibirKafka();
				tiempoEspera = Integer.parseInt(kafkaResult) * 1000;
				System.out.println("Tiempo asignado");
			}

			// Consulta si el usuario esta registrado
			topic = "Visitor";
			key = "entrar";
			value = "entrar;" + AliasVisitor + ";" + topicConsumer;
			enviarKafka(topic, key, value);
			kafkaResult = recibirKafka();
			System.out.println("Proceso de entrada hecho");
			if (kafkaResult.equals("entrar")) {
				dentroParque(AliasVisitor);
			}
		}
		catch (Exception e) {
			System.out.println("Error al introducir datos por consola. " + e.getMessage());
		}
	}

	public void salirParque() {
		String AliasVisitor = "", PWVisitor = "";
		String topic = "", key = "", value = "";
		String kafkaResult = "";
		InputStreamReader isr = new InputStreamReader(System.in);
		BufferedReader br = new BufferedReader(isr);

		try {
			System.out.println("Introduzca su Alias/ID: ");
			AliasVisitor = br.readLine();
			System.out.println("Introduzca su contrasenya: ");
			PWVisitor = br.readLine();

			// Comprobamos si esta en el mapa (si esta en el mapa esta registrado)
			topic = "Visitor";
			key = "salir";
			value = "salir;" + AliasVisitor + ";" + topicConsumer;
			enviarKafka(topic, key, value);
			kafkaResult = recibirKafka();
		}
		catch (IOException e ) {
			System.out.println("Error al introducir datos por consola");
		}
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
			
			//Socket skRegistro = new Socket(p_registryHost, Integer.parseInt(p_registryPort));
			//preparar Secure Socket
			System.setProperty("javax.net.ssl.trustStore", "sd.store");
			Socket skRegistro = ((SSLSocketFactory) SSLSocketFactory.getDefault()).createSocket(p_registryHost, Integer.parseInt(p_registryPort));

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
						System.out.println("Desea realizar alguna operacion mas? (s, n)");
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
						entrarParque();
						salir = 1;
					}
					else if (operacion == 4) {
						salirParque();
						System.out.println("Saliendo del parque...");
						salir = 1;
					}
				}
				cadena = "";
				op = "";
			}
		}
		catch (Exception e) {
			System.out.println("Error al introducir los datos por consola");
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
			System.out.println("Error al introducir los datos por consola");
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
		
        if (args.length < 5) {
            System.out.println("Se debe indicar la direccion y el puerto del registro" + 
            " y del gestor de colas");
            System.out.println("$./FWQ_Visitor host_registro puerto_registro " + 
			"host_gestorColas puerto_gestorColas topicConsumer");
			System.exit(-1);
        }
		RegistryHost = args[0];
		RegistryPort = args[1];
		QueueHandlerHost = args[2];
		QueueHandlerPort = args[3];
		topicConsumer = args[4];


		//preparar(QueueHandlerHost, QueueHandlerPort);
		// Kafka Producer
		FWQ_Visitor.ProducerProps.put("bootstrap.servers", QueueHandlerHost + ":" + QueueHandlerPort);
		FWQ_Visitor.ProducerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); 
		FWQ_Visitor.ProducerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		FWQ_Visitor.ProducerProps.put("max.block.ms", "1000");
		FWQ_Visitor.ProducerProps.put("delivery.timeout.ms", "1900");
		FWQ_Visitor.ProducerProps.put("linger.ms", "0");
		FWQ_Visitor.ProducerProps.put("request.timeout.ms", "50");
        FWQ_Visitor.producer = new KafkaProducer<String, String>(FWQ_Visitor.ProducerProps);

		// Kafka Consumer
		FWQ_Visitor.ConsumerProps.put("bootstrap.servers", QueueHandlerHost + ":" + QueueHandlerPort);
        FWQ_Visitor.ConsumerProps.put("group.id", topicConsumer);
        FWQ_Visitor.ConsumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        FWQ_Visitor.ConsumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		consumer = new KafkaConsumer<String, String>(FWQ_Visitor.ConsumerProps);
		// Suscribir el consumer a un topic
		consumer.subscribe(Collections.singletonList(topicConsumer));

		while(i == 0) {
			visitante.menu(RegistryHost, RegistryPort, QueueHandlerHost, QueueHandlerPort);
		}
    }
}