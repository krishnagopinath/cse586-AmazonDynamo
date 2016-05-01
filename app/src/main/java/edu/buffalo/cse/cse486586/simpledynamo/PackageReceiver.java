package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedInputStream;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
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
                        message = new Message(Stash.portStr, msg.source).RecoveryResponse(Stash.RecoveryMessages);
                        Stash.sendMessage(message);
                        break;
                    case RECOVERY_ACK:
                        for (String k : msg.RecoveryMessages.keySet()) {
                            Message recoveryMessage = msg.RecoveryMessages.get(k);

                            String key = recoveryMessage.key;
                            String value = recoveryMessage.value;

                            String position = Stash.getPosition(key);


                            if (position.equalsIgnoreCase(Stash.portStr)
                                    || Arrays.asList(Stash.predecessorMap.get(Stash.portStr)).contains(position)) {

                                ContentValues cv = new ContentValues();
                                cv.put("key", key);
                                cv.put("value", value);
                                Stash.sqlite.insertWithOnConflict("Msg", null, cv, SQLiteDatabase.CONFLICT_REPLACE);

                                //Stash.store.edit().putString(key, value).commit();
                            }

                        }
                        break;
                    case DELETE_REQ:
                        Stash.sqlite.delete("Msg", null, null);
                        //Stash.store.edit().clear().commit();
                        break;
                    case INSERT_OR:
                    case INSERT_REP:
                        long id = 0;

                        synchronized (Stash.sqlite) {
                            String key = msg.key;
                            String value = msg.value;
                            ContentValues cv = new ContentValues();
                            cv.put("key", key);
                            cv.put("value", value);

                            Stash.RecoveryMessages.put(key, msg);
                            id = Stash.sqlite.insertWithOnConflict("Msg", null,
                                    cv, SQLiteDatabase.CONFLICT_REPLACE);

                            //Stash.store.edit().putString(key, value).commit();

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


                        /*
                            Map<String, ?> rows = Stash.store.getAll();


                            for (String key : rows.keySet()) {
                                messages.put(key,  rows.get(key).toString());
                            }

                        */


                        message = new Message(Stash.portStr, msg.source).QueryAllResponse(messages);
                        Stash.sendMessage(message);


                        break;

                    case QUERY_ALL_ACK:
                        Iterator<Entry<String, String>> it = msg.QueryMessages.entrySet().iterator();
                        while (it.hasNext()) {
                            Entry entry = it.next();
                            Stash.matrixCursor.addRow(new Object[]{
                                    entry.getKey(),
                                    entry.getValue()
                            });

                        }
                        Stash.waitFlagger = true;
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


                    /*
                        String row = Stash.store.getString(selection, "");

                        if(!row.equals("")) {
                            mValue = row;
                        } else {
                            try {
                                Thread.sleep(100);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }

                        while (mValue.equals("")) {
                            row = Stash.store.getString(selection, "");
                            destination = msg.source;
                            mValue = row;
                        }
                    */

                        messages.put(mKey, mValue);
                        message = new Message(Stash.portStr, destination).QuerySelectionResponse(mKey, mValue, messages);


                        Stash.sendMessage(message);
                        break;
                    case QUERY_SEL_ACK:
                        Iterator<Entry<String, String>> it1 = msg.QueryMessages.entrySet()
                                .iterator();
                        while (it1.hasNext()) {

                            Entry pairs = (Entry) it1.next();
                            if (!(pairs.getValue().equals(""))) {
                                Stash.matrixCursor.addRow(new Object[]{
                                        pairs.getKey(), pairs.getValue()});
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