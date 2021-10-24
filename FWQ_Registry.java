import java.net.*;

/*
* Argumentos de llamada: 
*   · Puerto de escucha
*/

public class FWQ_Registry {

    @SuppressWarnings("resource")
    public static void main(String[] args) {
        String puerto = "";

        try {
            if (args.length < 1) {
                System.out.println("Debe indicar el puerto de escucha del servidor");
                System.out.println("$./Servidor puerto_servidor");
                System.exit(1);
            }
            
            puerto = args[0];
            ServerSocket skServidor = new ServerSocket(Integer.parseInt(puerto));
            System.out.println("Escucho el puerto: " + puerto);

            for (;;){
                Socket skCliente = skServidor.accept();
                System.out.println("Sirviendo cliente...");

                Thread t = new FWQ_HiloServidorRegistro(skCliente);
                t.start();
            }
        }
        catch (Exception e) {
            System.out.println("Error: " + e.toString());
        }
    }
}
