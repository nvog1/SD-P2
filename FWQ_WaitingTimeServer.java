import java.net.*;


public class FWQ_WaitingTimeServer {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		//parte de kafka
		


		//parte de socket server contra engine
		
		String puerto = "";
		
		try
		{
			if (args.length < 3) {
				System.out.println("Indica el puerto de escucha y la ip y puerto del broker(puerto puerto_broker ip_broker)");
				System.exit(1);
			}
			puerto = args[0];
			Thread t = new WTSHiloSck(puerto);
			
		}
		catch(Exception e)
		{
			System.out.println("Error: " + e.toString());
		}
		
	}
}