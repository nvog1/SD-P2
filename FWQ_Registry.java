import java.net.*;
import java.io.*;
import java.io.IOException;
import java.net.ServerSocket;
import javax.net.ssl.*;

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
                System.out.println("$./FWQ_Registry puerto_servidor");
                System.exit(1);
            }
            
            puerto = args[0];
            //sslsocket
            System.setProperty("javax.net.ssl.keyStore", "sd.store");
            System.setProperty("javax.net.ssl.keyStorePassword", "password");
            ServerSocket skServidor = ((SSLServerSocketFactory)SSLServerSocketFactory.getDefault()).createServerSocket(Integer.parseInt(puerto));
            System.out.println("Escucho el puerto: " + puerto);

            for (;;){
                Socket skCliente = skServidor.accept();
                System.out.println("Sirviendo cliente...");

                Thread t = new FWQ_HiloServidorRegistro(skCliente);
                t.start();
            }
        }
        catch (SocketException e) {
            System.out.println("Error: El visitante se ha desconectado");
        }
        catch (IOException e) {
            System.out.println("Error al manipular los sockets");
        }
    }
}
