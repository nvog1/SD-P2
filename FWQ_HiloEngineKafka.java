import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;
import java.lang.Exception;
import java.io.*;
import java.util.Properties;
import java.util.*;
import java.time.Duration;

public class FWQ_HiloEngineKafka extends Thread {
    private Properties ProducerProps = new Properties();
    private Properties ConsumerProps = new Properties();
    // hay que probar las 2 lineas siguientes
    private KafkaProducer<String, String> producer;
    private KafkaConsumer<String, String> consumer;


    public FWQ_HiloEngineKafka(String ipBroker, String puertoBroker) {
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
		consumer.subscribe(Collections.singletonList("SD"));
    }

	// GroupID para diferenciar los topics? (en o'reilly trata el groupID como el topic)
	/*public String leerKafka(String groupID) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "broker1:9092");
		props.put("group.id", groupID);
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(props);

		// Suscribir el consumer a un topic
		consumer.subscribe(Collections.singletonList(groupID));
	}*/
    
    // Comprueba si el Alias/ID esta registrado
    public boolean entrarSalir(String topic, String value) {
        boolean result = false;

        if (value == "0") {
            // El usuario quiere entrar al parque
        }
        return result;
    }

    public void procesarKafka(String topic, String key, String value) {
        // Topic muestra el ALias/ID del Visitor
        // Key muestra la accion que se quiere hacer
        // Value muestra la opcion a la accion que se quiere hacer

        System.out.println("Topic: " + topic + "; Key: " + key + "; Value: " + value);
        if (key == "entrarSalir") {
            // Se quiere entrar (value == "0") o salir (value == "1")
            entrarSalir(topic, value);
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
}