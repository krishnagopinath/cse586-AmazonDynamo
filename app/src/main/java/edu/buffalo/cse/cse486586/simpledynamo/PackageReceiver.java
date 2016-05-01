package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedInputStream;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class PackageReceiver implements Runnable {
    ServerSocket serverSocket = null;
    Message msg;

    PackageReceiver(ServerSocket serverSocket) {
        this.serverSocket = serverSocket;
    }

    private Socket socket = null;

    public void run() {
        try {
            while (true) {

                {
                    socket = serverSocket.accept();
                    ObjectInputStream ois = new ObjectInputStream(
                            new BufferedInputStream(socket.getInputStream()));
                    msg = (Message) ois.readObject();
                }

                Message message = null;
                HashMap<String, String> messages = null;


                switch (msg.getMessageStage()) {
                    case RECOVERY_REQ:
                        message = new Message(Stash.portStr, msg.getSource()).RecoveryResponse(Stash.RecoveryMessages);
                        Stash.sendMessage(message);
                        break;
                    case RECOVERY_ACK:
                        for (String k : msg.getRecoveryMessages().keySet()) {
                            Message recoveryMessage = msg.getRecoveryMessages().get(k);

                            String key = recoveryMessage.getKey();
                            String value = recoveryMessage.getValue();

                            String position = Stash.getPosition(key);


                            if (position.equalsIgnoreCase(Stash.portStr)
                                    || Arrays.asList(Stash.predecessorMap.get(Stash.portStr)).contains(position)) {

                                Stash.store.edit().putString(key, value).commit();
                            }

                        }
                        break;
                    case DELETE_REQ:
                        Stash.store.edit().clear().commit();
                        break;

                    case INSERT_REQ:
                        synchronized (Stash.lock) {
                            String key = msg.getKey();
                            String value = msg.getValue();

                            Stash.RecoveryMessages.put(key, msg);
                            Stash.store.edit().putString(key, value).commit();

                        }
                        break;

                    case QUERY_ALL:
                        messages = new HashMap<String, String>();

                        Map<String, ?> rows = Stash.store.getAll();

                        for (String key : rows.keySet()) {
                            messages.put(key, rows.get(key).toString());
                        }

                        message = new Message(Stash.portStr, msg.getSource()).QueryResponse(messages);
                        Stash.sendMessage(message);
                        break;

                    case QUERY_SEL:
                        String destination = "";
                        String mKey = "";
                        String mValue = "";
                        messages = new HashMap<String, String>();

                        String selection = msg.getKey();

                        mKey = msg.getKey();
                        mValue = "";

                        destination = msg.getSource();

                        String row = Stash.store.getString(selection, "");

                        if (!row.equals("")) {
                            mValue = row;
                        } else {
                            Thread.sleep(100);
                        }

                        while (mValue.equals("")) {
                            row = Stash.store.getString(selection, "");
                            destination = msg.getSource();
                            mValue = row;
                        }

                        messages.put(mKey, mValue);
                        message = new Message(Stash.portStr, destination).QueryResponse(messages);
                        Stash.sendMessage(message);

                        break;

                    case QUERY_ACK:

                        for (String key : msg.getQueryMessages().keySet()) {
                            if (!msg.getQueryMessages().get(key).equals("")) {
                                Stash.matrixCursor.addRow(new Object[]{
                                        key, msg.getQueryMessages().get(key)
                                });
                            }
                        }
                        Stash.waitFlagger = true;
                        break;


                }

            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}