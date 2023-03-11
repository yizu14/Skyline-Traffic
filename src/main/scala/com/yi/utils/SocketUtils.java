package com.yi.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.net.Socket;

public class SocketUtils {

    public void sendFile(Reader reader){
        Socket socket = null;
        try {
            socket = new Socket("192.168.1.43",5000);
        } catch (IOException e) {
            e.printStackTrace();
        }
        BufferedReader br = new BufferedReader(reader);
        try {
            OutputStream os = socket.getOutputStream();
            String buffer = "";
            while((buffer = br.readLine())!= null){
                buffer += "\r\n";
                os.write(buffer.getBytes());
            }
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
