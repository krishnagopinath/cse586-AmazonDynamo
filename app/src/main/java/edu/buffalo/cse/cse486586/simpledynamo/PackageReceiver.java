package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedInputStream;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import android.content.ContentValues;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.util.Log;

public class PackageReceiver implements Runnable {
    ServerSocket serverSocket = null;
    Message msg;

    PackageReceiver(ServerSocket sc) {
        this.serverSocket = sc;
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


                switch (msg.MessageStage) {
                    case RECOVERY_REQ:
                        message = new Message(Stash.portStr, msg.source).RecoveryResponse(Stash.table);
                        Stash.sendMessage(message);
                        break;
                    case RECOVERY_ACK:
                        for (String k : msg.RecoveryMessages.keySet()) {
                            Message m = msg.RecoveryMessages.get(k);

                            String ke = m.key;
                            String v = m.value;

                            long id;
                            String position = "";
                            try {
                                if (Stash.nodeList.higherEntry(genHash(ke)) != null)
                                    position = Integer.toString(Stash.nodeList.higherEntry(
                                            genHash(ke)).getValue());
                                else
                                    position = Integer.toString(Stash.nodeList.firstEntry()
                                            .getValue());
                            } catch (NoSuchAlgorithmException e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                            }



                            if (position.equalsIgnoreCase(Stash.portStr)
                                    || Arrays.asList(Stash.predecessorMap.get(Stash.portStr)).contains(position)) {
                                ContentValues cv = new ContentValues();
                                cv.put("key", ke);
                                cv.put("value", v);
                                Stash.sqlite.insertWithOnConflict(
                                        "Msg", null, cv,
                                        SQLiteDatabase.CONFLICT_REPLACE);
                            }

                        }
                        break;
                    case DELETE_REQ:
                        Stash.sqlite.delete("Msg", null, null);
                        break;
                    case INSERT_OR:
                    case INSERT_REP:
                        long id = 0;

                        synchronized (Stash.sqlite) {
                            String k = msg.key;
                            String v = msg.value;
                            ContentValues cv = new ContentValues();
                            cv.put("key", k);
                            cv.put("value", v);
                            Log.v("Insert", "Insert received at "
                                    + Stash.portStr);
                            Log.v("Insert", k + " =key " + v + " =value" + " State= "
                                    + msg.MessageStage);
                            Stash.table.put(k, msg);
                            id = Stash.sqlite.insertWithOnConflict("Msg", null,
                                    cv, SQLiteDatabase.CONFLICT_REPLACE);

                            Log.v("Insertion finally done", Long.toString(id));
                        }
                        break;
                    case QUERY_ALL:


                        messages = new HashMap<String, String>();
                        Cursor cursor = Stash.sqlite.query(
                                "Msg", new String[]{"key", "value"}, null, null, null, null, null);


                        while (cursor.moveToNext()) {
                            int key = cursor.getColumnIndex("key");
                            int value = cursor.getColumnIndex("value");
                            messages.put(cursor.getString(key),
                                    cursor.getString(value));
                        }


                        Log.v("PackageReceiver Query", "Returning cursor");

                        message = new Message(Stash.portStr, msg.source).QueryAllResponse(messages);

                        Stash.sendMessage(message);


                        break;

                    case QUERY_ALL_ACK:
                        Stash.hashmapstar.putAll(msg.QueryMessages);
                        Log.v("PackageReceiver Query", "hashmap updated " + msg.QueryMessages);
                        Iterator<Entry<String, String>> it = msg.QueryMessages.entrySet().iterator();
                        Log.v("Provider query", "hashmap here " + msg.QueryMessages);
                        while (it.hasNext()) {
                            @SuppressWarnings("rawtypes")
                            Entry pairs = (Entry) it.next();
                            Stash.matcursor.addRow(new Object[]{
                                    pairs.getKey(), pairs.getValue()});
                            Log.v("provider query", "matcursor updated " + pairs.getValue());

                        }
                        Stash.tempflag = true;
                        Log.v("PackageReceiver query", "Cursor updated lock released");
                        break;

                    case QUERY_SEL:
                        String destination = "";
                        String mKey = "";
                        String mValue = "";
                        messages = new HashMap<String, String>();

                        String[] columns = {"key", "value"};
                        String selection = msg.key;

                        mKey = msg.key;
                        mValue = "";

                        Cursor cursor1 = Stash.sqlite.query("Msg",
                                columns, "key = " + "'" + selection + "'", null, null,
                                null, null);

                        destination = msg.source;
                        cursor1.moveToNext();
                        int key = cursor1.getColumnIndex("key");
                        int value = cursor1.getColumnIndex("value");
                        if (cursor1.moveToFirst()) {
                            mValue = cursor1.getString(value);
                        } else
                            try {
                                Thread.sleep(100);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }

                        while (mValue.equals(null)) {
                            cursor1 = Stash.sqlite.query("Msg", columns,
                                    "key = " + "'" + selection + "'", null, null, null,
                                    null);

                            destination = msg.source;
                            cursor1.moveToNext();
                            key = cursor1.getColumnIndex("key");
                            value = cursor1.getColumnIndex("value");
                            mValue = cursor1.getString(value);
                        }

                        Log.v("PackageReceiverQuery", "Key :" + mKey + " Value: "
                                + mValue + " in " + Stash.portStr);


                        messages.put(mKey, mValue);

                        message = new Message(Stash.portStr, destination).QuerySelectionResponse(mKey, mValue, messages);

                        Log.v("PackageReceiver Query", "Returning cursor");


                        Stash.sendMessage(message);
                        break;
                    case QUERY_SEL_ACK:
                        Stash.hashmap.putAll(msg.QueryMessages);
                        Log.v("PackageReceiver Query", "hashmap updated " + msg.QueryMessages);
                        Iterator<Entry<String, String>> it1 = msg.QueryMessages.entrySet()
                                .iterator();
                        Log.v("Provider query", "hashmap here " + msg.QueryMessages);
                        while (it1.hasNext()) {
                            @SuppressWarnings("rawtypes")
                            Entry pairs = (Entry) it1.next();
                            if (!(pairs.getValue().equals(""))) {
                                Stash.matcursor.addRow(new Object[]{
                                        pairs.getKey(), pairs.getValue()});
                            }
                            Log.v("provider query", "matcursor updated " + pairs.getKey());

                        }
                        Stash.tempflag = true;
                        Log.v("PackageReceiver query", "Cursor updated lock released");
                        break;


                }

            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        @SuppressWarnings("resource")
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }


}