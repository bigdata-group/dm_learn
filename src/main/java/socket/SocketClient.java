package socket;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Socket;

/**
 * Created by jiwenlong on 2018/12/27.
 */
public class SocketClient {
    public static void main(String[] args) throws Exception{
        Socket socket = null;
        DataInputStream dis = null;
        InputStream is = null;

        try {
            socket = new Socket("localhost", 9000);
            BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            while (true) {
                System.out.println("waiting.....");
                System.out.println("receive_msg:" + br.readLine());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
