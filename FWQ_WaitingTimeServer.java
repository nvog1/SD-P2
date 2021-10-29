import java.net.*;


public class FWQ_WaitingTimeServer {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		//parte de kafka
		Properties props = new Properties();
		props.put("bootstrap.servers", "broker1:9092,broker2:9092");
		props.put("group.id", "CountryCounter");
		props.put("key.deserializer",
			"org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer",
			"org.apache.kafka.common.serialization.StringDeserializer");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);


		//parte de socket server contra engine
		
		String puerto = "";
		
		try
		{
			if (args.length < 3) {
				System.out.println("Indica el puerto de escucha y la ip y puerto del broker(puerto puerto_broker ip_broker)");
				System.exit(1);
			}
			puerto = args[0];
			Thread t = new WTSHilo(puerto);
			
		}
		catch(Exception e)
		{
			System.out.println("Error: " + e.toString());
		}
		
	}
}