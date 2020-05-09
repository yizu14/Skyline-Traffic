package com.yi.utils;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.SCPClient;

import java.io.IOException;

public class SCPUtils {
    private String serverIP = "192.168.1.43";
    private String userName = "lijiaqi";
    private String password = "199888";
    private String destDir = "C:/Users/lijiaqi/Desktop/tar/Data";
    private String localFile;

    public SCPUtils(String localFile){
        this.localFile = localFile;
    }
    public void scpTo(){
        Connection conn = new Connection(serverIP);
        try {
            conn.connect();
            boolean b = conn.authenticateWithPassword(userName, password);
            SCPClient client = new SCPClient(conn);
            client.put(localFile,destDir);
            conn.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
