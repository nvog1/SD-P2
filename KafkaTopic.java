import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.*;

public class KafkaTopic {
    public KafkaTopic (String ipBroker, String puertoBroker, String nombreTopic) {
        
        try {
            Properties props = new Properties();
            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, ipBroker + ":" + puertoBroker);
            AdminClient admin = AdminClient.create(props);

            // Creando el nuevo topic
            NewTopic topic = new NewTopic(nombreTopic, 1, (short) 1);
            List<NewTopic> newTopics = new ArrayList<topic>();
            newTopics.add(newTopic);
            adminClient.createTopics(newTopic);
            //admin.createTopics(Collections.singleton(topic));
            System.out.println("Topic creado.");

            // Listar los topics existentes
            System.out.println("Topics listados: ");
            admin.listTopics().names().get().forEach(System.out::println);
        }
        catch (Exception e) {
            System.out.println("Error: " + e.toString());
        }
    }
}

/*Properties properties = new Properties();
properties.load(new FileReader(new File("kafka.properties")));

AdminClient adminClient = AdminClient.create(properties);
NewTopic newTopic = new NewTopic("topicName", 1, (short)1); //new NewTopic(topicName, numPartitions, replicationFactor)

List<NewTopic> newTopics = new ArrayList<NewTopic>();
newTopics.add(newTopic);

adminClient.createTopics(newTopics);
adminClient.close();*/