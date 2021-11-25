import java.net.*;


public class FWQ_WaitingTimeServer {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		String puerto = "";
		String ipBroker = "";
		String puertoBroker = "";
		
		try
		{
			if (args.length < 3) {
				System.out.println("Indica el puerto de escucha y la ip y puerto del broker(puerto ip_broker puerto_broker)");
				System.exit(1);
			}
			puerto = args[0];
			ipBroker = args[1];
			puertoBroker = args[2];

			//sck thread
			Thread tsck = new WTSHiloSck(puerto);
			tsck.start();

			//kafka thread
			Thread tkafka = new WTSHiloKafka(ipBroker, puertoBroker);
			tkafka.start();
			
		}
		catch(Exception e)
		{
			System.out.println("Error: " + e.toString());
		}
		
	}
}