import java.net.*

public class FWQ_Engine {

	private string ip_broker;
	private string puerto_broker;
	private string ip_wts;
	private string puerto_wts;
	private int maxVisitantes;
	private int segundos;//segundos de espera entre peticiones al wts

	public static void main(String[] args) {
		
		try
		{
			if (args.length < 6) {
				System.out.println("Indica: ip_broker puerto_broker ip_wts puerto_wts maxVisitantes segundos");
				System.exit(1);
			}
			ip_broker = args[0];
			puerto_broker = args[1];
			ip_wts = args[2];
			puerto_wts = args[3];
			maxVisitantes = args[4];
			segundos = args[5];

			//conexion a kafka

			//conexion a wts
			
		}
		catch(Exception e)
		{
			System.out.println("Error: " + e.toString());
		}


	}
}
