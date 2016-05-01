package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedInputStream;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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
                if (msg.type.equalsIgnoreCase("insert")) {
                    insert(msg);
                }
                if (msg.type.equalsIgnoreCase("query")) {
                    query(msg);
                }
                if (msg.type.equalsIgnoreCase("cursor")) {
                    sendbackCursor(msg);
                }
                if (msg.type.equalsIgnoreCase("tableReq")) {
                    sendTable(msg);
                }
                if (msg.type.equalsIgnoreCase("table")) {
                    copyContents2(msg);
                }
                if (msg.type.equalsIgnoreCase("delete")) {
                    delete();
                }

            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void delete() {

        Stash.sqlite.delete("Msg", null, null);
    }

    private void copyContents2(Message msg2) {

        for (String k : msg2.hashtable.keySet()) {
            Message m = new Message();
            m = msg2.hashtable.get(k);
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
                    || position.equalsIgnoreCase(Stash.pred1)
                    || position.equalsIgnoreCase(Stash.pred2)) {
                ContentValues cv = new ContentValues();
                cv.put("key", ke);
                cv.put("value", v);
                Stash.sqlite.insertWithOnConflict(
                        "Msg", null, cv,
                        SQLiteDatabase.CONFLICT_REPLACE);
            }

        }
    }


    private void sendTable(Message msg2) {

        Message mCursor = new Message().RecoveryResponse(Stash.portStr, msg2.source, Stash.table);

        Runnable r = new PackageSender(mCursor);
        Thread t = new Thread(r);
        t.start();

    }

    private void sendbackCursor(Message msg2) {

        if (msg2.Querytype.equalsIgnoreCase("basic")) {
            Stash.hashmap.putAll(msg2.hashmap);
            Log.v("PackageReceiver Query", "hashmap updated " + msg2.hashmap);
            Iterator<Entry<String, String>> it = msg2.hashmap.entrySet()
                    .iterator();
            Log.v("Provider query", "hashmap here " + msg2.hashmap);
            while (it.hasNext()) {
                @SuppressWarnings("rawtypes")
                Entry pairs = (Entry) it.next();
                if (!(pairs.getValue().equals(""))) {
                    Stash.matcursor.addRow(new Object[]{
                            pairs.getKey(), pairs.getValue()});
                }
                Log.v("provider query", "matcursor updated " + pairs.getKey());

            }
            Stash.tempflag = true;
            Log.v("PackageReceiver query", "Cursor updated lock released");
        } else if (msg2.Querytype.equalsIgnoreCase("all")) {
            Stash.hashmapstar.putAll(msg2.hashmap);
            Log.v("PackageReceiver Query", "hashmap updated " + msg2.hashmap);
            Iterator<Entry<String, String>> it = msg2.hashmap.entrySet()
                    .iterator();
            Log.v("Provider query", "hashmap here " + msg2.hashmap);
            while (it.hasNext()) {
                @SuppressWarnings("rawtypes")
                Entry pairs = (Entry) it.next();
                Stash.matcursor.addRow(new Object[]{
                        pairs.getKey(), pairs.getValue()});
                Log.v("provider query", "matcursor updated " + pairs.getValue());

            }
            Stash.tempflag = true;
            Log.v("PackageReceiver query", "Cursor updated lock released");
        }
    }

    private void query(Message msg2) {


        if (msg2.Querytype.equalsIgnoreCase("basic")) {

            String destination = "";
            String mKey = "";
            String mValue = "";
            HashMap<String, String> messages = new HashMap<String, String>();

            String[] columns = {"key", "value"};
            String selection = msg2.key;

            mKey = msg2.key;
            mValue = "";

            Cursor cursor = Stash.sqlite.query("Msg",
                    columns, "key = " + "'" + selection + "'", null, null,
                    null, null);

            destination = msg2.source;
            cursor.moveToNext();
            int key = cursor.getColumnIndex("key");
            int value = cursor.getColumnIndex("value");
            if (cursor.moveToFirst()) {
                mValue = cursor.getString(value);
            } else
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            while (mValue.equals(null)) {
                cursor = Stash.sqlite.query("Msg", columns,
                        "key = " + "'" + selection + "'", null, null, null,
                        null);

                destination = msg2.source;
                cursor.moveToNext();
                key = cursor.getColumnIndex("key");
                value = cursor.getColumnIndex("value");
                mValue = cursor.getString(value);
            }

            Log.v("PackageReceiverQuery", "Key :" + mKey + " Value: "
                    + mValue + " in " + Stash.portStr);


            messages.put(mKey, mValue);

            Message message = new Message().QuerySelectionResponse(Stash.portStr, destination, mKey, mValue, messages);

            Log.v("PackageReceiver Query", "Returning cursor");
            Runnable r = new PackageSender(message);
            Thread th = new Thread(r);
            th.start();

        } else if (msg2.Querytype.equalsIgnoreCase("all")) {

            HashMap<String, String> messages = new HashMap<String, String>();
            try {
                Cursor cursor = Stash.sqlite.query(
                        "Msg", new String[]{"key", "value"}, null, null, null, null, null);


                while (cursor.moveToNext()) {
                    int key = cursor.getColumnIndex("key");
                    int value = cursor.getColumnIndex("value");
                    messages.put(cursor.getString(key),
                            cursor.getString(value));
                }
                ;

                Log.v("PackageReceiver Query", "Returning cursor");

                Message message = new Message().QueryAllResponse(Stash.portStr, msg2.source, messages);

                Runnable r = new PackageSender(message);
                Thread th = new Thread(r);
                th.start();
            } catch (Exception e) {
            }

        }
    }


    private void insert(Message msg2) {
        long id = 0;

        synchronized (Stash.sqlite) {
            String k = msg2.key;
            String v = msg2.value;
            ContentValues cv = new ContentValues();
            cv.put("key", k);
            cv.put("value", v);
            Log.v("Insert", "Insert received at "
                    + Stash.portStr);
            Log.v("Insert", k + " =key " + v + " =value" + " State= "
                    + msg2.state);
            Stash.table.put(k, msg2);
            id = Stash.sqlite.insertWithOnConflict("Msg", null,
                    cv, SQLiteDatabase.CONFLICT_REPLACE);

            Log.v("Insertion finally done", Long.toString(id));
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