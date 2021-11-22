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
        maxVisitantes = aforo;

        this.ProducerProps.put("bootstrap.servers", ipBroker + ":" + puertoBroker);
        this.ProducerProps.put("key.serializer" , "org.apache.kafka.common.serialization.StringSerializer");
        this.ProducerProps.put("value.serializer" , "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<String, String>(ProducerProps);

        this.ConsumerProps.put("bootstrap.servers", ipBroker + ":" + puertoBroker);
        this.ConsumerProps.put("group.id", "Visitors");
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
        Integer resultado = -1;

        /*try {
            Connection connection = DriverManager.getConnection(CONNECTIONURL, USER, PASSWORD);
            
            Statement statement = connection.createStatement();
            String sentence = "SELECT * FROM Usuarios");
            ResultSet result = statement.executeQuery(sentence);
            resultado = result.last().getRow();
        }
        catch (Exception e) {
            System.out.println("Error SQL: " + e.getMessage());
        }*/

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

            // TODO: Consultar aforo del parque
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

    public void procesarKafka(String topic, String key, String value) {
        // Topic muestra el ALias/ID del Visitor
        // Key muestra la accion que se quiere hacer
        // Value muestra la opcion a la accion que se quiere hacer

        System.out.println("Topic: " + topic + "; Key: " + key + "; Value: " + value);
        if (key == "entrarSalir") {
            // Se quiere entrar (value == "0") o salir (value == "1")
            if (entrarSalir(topic, value)) {
                // El usuario esta registrado
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, "entrar");

                try {
                    // Aqui hay un warning que podemos obviar
                    producer.send(record, new DemoProducerCallback());
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

    
	private static class DemoProducerCallback implements Callback {
		@Override
		public void onCompletion(RecordMetadata recordMetadata, Exception e){
			if (e != null) {
				e.printStackTrace();
			}
		}
	}
}