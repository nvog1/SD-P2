import java.net.*;

public class FWQ_WaitingTimeServer {

	public static void main(String[] args) {

		//parte de kafka



		//parte de socket server contra engine
		
		String puerto = "";
		
		try
		{
			if (args.length < 2) {
				System.out.println("Indica el puerto de escucha y la ip y puerto del broker(puerto puerto:ip)");
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