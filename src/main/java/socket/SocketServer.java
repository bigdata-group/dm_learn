package socket;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by jiwenlong on 2018/12/27.
 */
public class SocketServer {
    public static void main(String[] args) throws Exception {
        ServerSocket server = new ServerSocket(8800);
        Socket socket = server.accept();
        PrintWriter pw = new PrintWriter(socket.getOutputStream());
        //DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        while (true){
            String str = br.readLine();
            if (str != null) {
                System.out.println("send:"+str);
                pw.write(str + "\n");
                //out.writeUTF(str);
                //out.flush();
                pw.flush();
            }
        }
    }
}
