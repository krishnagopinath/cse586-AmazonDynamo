package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedOutputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;

public class PackageSender implements Runnable {


    Message message;
    Socket socket;

    PackageSender(Message msg) {
        this.message = msg;
    }

    public void run() {

        try {

            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(message.getReceiver()) * 2);
            socket.setSoTimeout(2000);
            OutputStream outputStream = socket.getOutputStream();
            BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(outputStream);


            ObjectOutputStream objectOutputStream = new ObjectOutputStream(bufferedOutputStream);
            objectOutputStream.writeObject(message);
            objectOutputStream.flush();
            outputStream.flush();
            outputStream.close();
            objectOutputStream.close();
            socket.close();
        } catch (SocketException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
