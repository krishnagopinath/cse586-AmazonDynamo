package edu.buffalo.cse.cse486586.simpledynamo;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.util.LinkedList;
import java.util.Map;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.telephony.TelephonyManager;


public class SimpleDynamoProvider extends ContentProvider {

    double delay = 0;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {

        Stash.store.edit().clear().commit();

        for (String key : Stash.nodeHashMap.keySet()) {
            Message msg = new Message(Stash.portStr, Stash.nodeHashMap.get(key)).DeleteRequest();
            Stash.sendMessage(msg);
        }

        return 0;
    }


    @Override
    public String getType(Uri uri) {
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        try {

            String key = values.getAsString("key");
            String value = values.getAsString("value");
            String destination = "";


            //find right location
            destination = Stash.getPosition(key);

            Message m = new Message(Stash.portStr, destination).InsertRequest(key, value);

            Stash.RecoveryMessages.put(m.getKey(), m);

            //insert it in destination first
            Stash.sendMessage(m, Thread.MAX_PRIORITY);

            //find replicas for destination
            String[] replicas = Stash.successorMap.get(m.getReceiver());

            Message m2 = new Message(Stash.portStr, replicas[0]).InsertRequest(m.getKey(), m.getValue());
            Message m3 = new Message(Stash.portStr, replicas[1]).InsertRequest(m.getKey(), m.getValue());

            //send to first replica
            Stash.sendMessage(m2, Thread.MAX_PRIORITY);

            //send to second replica
            Stash.sendMessage(m3, Thread.MAX_PRIORITY);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public boolean onCreate() {
        try {
            Stash.context = getContext();

            Stash.store = Stash.context.getSharedPreferences(Stash.PREFS_NAME, 0);


            TelephonyManager tel = (TelephonyManager) Stash.context
                    .getSystemService(Context.TELEPHONY_SERVICE);
            Stash.portStr = tel.getLine1Number().substring(
                    tel.getLine1Number().length() - 4);


            //set up ring structure for later use
            LinkedList<String> nodeRing = new LinkedList<String>();
            nodeRing.add("5562");
            nodeRing.add("5556");
            nodeRing.add("5554");
            nodeRing.add("5558");
            nodeRing.add("5560");


            for (int i = 0; i < Stash.PORTS.length; i++) {
                //generate hashes and keep in node list for comparisons
                Stash.nodeHashMap.put(Stash.genHash(Stash.PORTS[i]), Stash.PORTS[i]);

                int position = nodeRing.indexOf(Stash.PORTS[i]);

                //set predecessors
                Stash.predecessorMap.put(Stash.PORTS[i], new String[]{
                        nodeRing.get((position - 1 + 5) % 5),
                        nodeRing.get((position - 2 + 5) % 5)});

                //set successors
                Stash.successorMap.put(Stash.PORTS[i], new String[]{
                        nodeRing.get((position + 1) % 5),
                        nodeRing.get((position + 2) % 5)});
            }

            ServerSocket serverSocket = new ServerSocket();
            serverSocket.setReuseAddress(true);
            serverSocket.bind(new InetSocketAddress(Integer.parseInt(Stash.SERVER_PORT)));

            new Thread(new PackageReceiver(serverSocket)).start();
            Thread.sleep(1000);

            synchronized (Stash.LOCK) {
                //ask for messages
                for (String key : Stash.nodeHashMap.keySet()) {
                    Message msg = new Message(Stash.portStr, Stash.nodeHashMap.get(key)).RecoveryRequest();
                    Stash.sendMessage(msg, Thread.MIN_PRIORITY);
                }

            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public synchronized Cursor query(Uri uri, String[] projection,
                                     String selection, String[] selectionArgs, String sortOrder) {
        try {
            if (selection.equals("@")) {
                Stash.RecoveryMessages.clear();
                Thread.sleep(6000);

                Map<String, ?> rows = Stash.store.getAll();
                MatrixCursor cursor = new MatrixCursor(new String[]{Stash.KEY_FIELD, Stash.VALUE_FIELD});
                for (String key : rows.keySet()) {
                    cursor.addRow(new Object[]{key, rows.get(key).toString()});
                }

                return cursor;


            } else if (selection.equals("*")) {
                Stash.waitFlagger = false;
                Stash.matrixCursor = new MatrixCursor(new String[]{"key", "value"});
                for (String key : Stash.nodeHashMap.keySet()) {
                    Message msg = new Message(Stash.portStr, Stash.nodeHashMap.get(key)).QueryAll();
                    Stash.sendMessage(msg);
                }
                Thread.sleep(6000);

                while (!Stash.waitFlagger) {
                    Thread.sleep(100);
                }
                return Stash.matrixCursor;
            } else {
                Stash.waitFlagger = false;
                Stash.matrixCursor = new MatrixCursor(new String[]{"key", "value"});

                String destination = Stash.getPosition(selection);
                Message msg = new Message(Stash.portStr, destination).QuerySelection(selection);
                Stash.sendMessage(msg);

                //also ask from replicas
                destination = Stash.successorMap.get(msg.getReceiver())[0];

                msg = new Message(Stash.portStr, destination).QuerySelection(selection);
                Stash.sendMessage(msg);

                while (!Stash.waitFlagger) {
                    Thread.sleep(100);
                    if (++delay > 10000) {
                        break;
                    }
                }

                delay = 0;
                Thread.sleep(1000);
                Stash.matrixCursor.moveToFirst();

                return Stash.matrixCursor;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }


    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        return 0;
    }


}