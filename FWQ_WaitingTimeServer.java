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
			ServerSocket ss = new ServerSocket(Integer.parseInt(puerto));
			System.out.println("Escucho el puerto " + puerto);

			for(;;)
			{
				Socket cs = ss.accept();
				System.out.println("Sirviendo al motor...");

				Thread t = new WTSHilo(cs);
				t.start();
			}
		}
		catch(Exception e)
		{
			System.out.println("Error: " + e.toString());
		}
		
	}
}