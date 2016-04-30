package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedOutputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;

import android.util.Log;

public class PackageSender implements Runnable {


    Message m;
    Socket socket;

    PackageSender(Message msg) {
        this.m = msg;

    }

    public void run() {

        try {
            int port = Integer.parseInt(m.destination) * 2;
            Log.v("PackageSender", Integer.toString(port));
            socket = new Socket("10.0.2.2", port);
            socket.setSoTimeout(2000);
            OutputStream o = socket.getOutputStream();
            BufferedOutputStream bout = new BufferedOutputStream(o);


            ObjectOutputStream oos = new ObjectOutputStream(bout);
            oos.writeObject(m);
            oos.flush();
            o.flush();
            o.close();
            oos.close();
            socket.close();
        } catch (SocketException se) {
            Log.d("PackageSender time out", "Time out at " + m.destination);
        } catch (Exception e) {
            Log.v("Clienttask", e.toString());
        } finally {
            try {


            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

}
