package edu.buffalo.cse.cse486586.simpledynamo;

import java.net.ServerSocket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.TreeMap;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.telephony.TelephonyManager;
import android.util.Log;


/*NodeList : 5562 --> 5556 --> 5554 --> 5558 --> 5560*/
public class SimpleDynamoProvider extends ContentProvider {

    double wait = 0;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {

        Stash.sqlite.delete("Msg", null, null);

        for (String k : Stash.nodeList.keySet())

        {
            Message msg = new Message(Stash.portStr, Integer.toString(Stash.nodeList.get(k))).DeleteRequest();

            Log.v("Delete *", "sending to " + Stash.nodeList.get(k));



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
            if (Stash.nodeList.higherEntry(genHash(key)) != null)
                destination = Integer.toString(Stash.nodeList.higherEntry(
                        genHash(key)).getValue());
            else
                destination = Integer.toString(Stash.nodeList.firstEntry()
                        .getValue());


            Log.v("Insert provider", " Destination is " + destination);


            Message m = new Message(Stash.portStr, destination).InsertOriginal(key, value);

            Stash.table.put(m.key, m);


            //insert it in destination first
            Stash.sendMessage(m, Thread.MAX_PRIORITY);

            Message m2 = null;
            Message m3 = null;

            //check destination and find replicas for destination
            if (m.destination.equalsIgnoreCase("5554")) {

                m2 = new Message(Stash.portStr, "5560").InsertReplica(m.key, m.value, "5560", "5558");
                m3 = new Message(Stash.portStr, "5558").InsertReplica(m.key, m.value, "5560", "5558");

            } else if (m.destination.equalsIgnoreCase("5556")) {
                m2 = new Message(Stash.portStr, "5558").InsertReplica(m.key, m.value, "5554", "5558");
                m3 = new Message(Stash.portStr, "5554").InsertReplica(m.key, m.value, "5554", "5558");

            } else if (m.destination.equalsIgnoreCase("5558")) {
                m2 = new Message(Stash.portStr, "5560").InsertReplica(m.key, m.value, "5560", "5562");
                m3 = new Message(Stash.portStr, "5562").InsertReplica(m.key, m.value, "5560", "5562");

            } else if (m.destination.equalsIgnoreCase("5560")) {

                m2 = new Message(Stash.portStr, "5562").InsertReplica(m.key, m.value, "5562", "5556");
                m3 = new Message(Stash.portStr, "5556").InsertReplica(m.key, m.value, "5556", "5562");

            } else if (m.destination.equalsIgnoreCase("5562")) {

                m2 = new Message(Stash.portStr, "5554").InsertReplica(m.key, m.value, "5556", "5554");
                m3 = new Message(Stash.portStr, "5556").InsertReplica(m.key, m.value, "5556", "5554");

            }


            //send to first replica
            Log.v("Replication ", "Sending " + m2.key + "  to "
                    + m.destination);

            Stash.sendMessage(m2, Thread.MAX_PRIORITY);

            //send to second replica
            Log.v("Replication ", "Sending " + m3.key + "  to "
                    + m.destination);

            Stash.sendMessage(m3, Thread.MAX_PRIORITY);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // try{Thread.sleep(6000);}catch(Exception e){}
        }
        return null;
    }

    @Override
    public boolean onCreate() {
        Stash.mydb = new DB(getContext());
        Stash.sqlite = Stash.mydb.getWritableDatabase();
        Stash.context = getContext();


        Stash.myContentResolver = Stash.context.getContentResolver();
        TelephonyManager tel = (TelephonyManager) Stash.context
                .getSystemService(Context.TELEPHONY_SERVICE);
        Stash.portStr = tel.getLine1Number().substring(
                tel.getLine1Number().length() - 4);


        //set predecessors
        if (Stash.portStr.equalsIgnoreCase("5554")) {
            Stash.pred1 = "5556";
            Stash.pred2 = "5562";
        }
        if (Stash.portStr.equalsIgnoreCase("5556")) {
            Stash.pred1 = "5562";
            Stash.pred2 = "5560";
        }
        if (Stash.portStr.equalsIgnoreCase("5558")) {
            Stash.pred1 = "5554";
            Stash.pred2 = "5556";
        }
        if (Stash.portStr.equalsIgnoreCase("5560")) {
            Stash.pred1 = "5558";
            Stash.pred2 = "5554";
        }
        if (Stash.portStr.equalsIgnoreCase("5562")) {
            Stash.pred1 = "5560";
            Stash.pred2 = "5558";
        }


        try {

            //generate hashes and keep in nodelist
            Stash.nodeList.put(genHash(Integer.toString(5554)), 5554);
            Stash.nodeList.put(genHash(Integer.toString(5556)), 5556);
            Stash.nodeList.put(genHash(Integer.toString(5558)), 5558);
            Stash.nodeList.put(genHash(Integer.toString(5560)), 5560);
            Stash.nodeList.put(genHash(Integer.toString(5562)), 5562);

            ServerSocket serverSocket = new ServerSocket(10000);

            new Thread(new PackageReceiver(serverSocket)).start();



            Thread.sleep(1000);

            synchronized (Stash.sqlite) {
                //ask for messages
                for (String k : Stash.nodeList.keySet()) {
                    Message msg = new Message(Stash.portStr, Integer.toString(Stash.nodeList.get(k))).RecoveryRequest();

                    Log.v("Recovery Query *",
                            "sending to " + Stash.nodeList.get(k));

                    Stash.sendMessage(msg, Thread.MIN_PRIORITY);
                }

            }

        } catch (Exception e) {
        }
        return false;
    }

    @Override
    public synchronized Cursor query(Uri uri, String[] projection,
                                     String selection, String[] selectionArgs, String sortOrder) {
        try {
            // Thread.sleep(7000);

            Log.i(Stash.TAG, "Query key: " + selection);
            if (selection.equals("@")) {
                Stash.table.clear();
                Thread.sleep(6000);
                return Stash.sqlite.query("Msg", null, null, null, null, null, null);
            }
            if (selection.equals("*")) {
                Stash.cursor = null;
                Stash.tempflag = false;
                Stash.matcursor = new MatrixCursor(new String[]{"key", "value"});
                for (String k : Stash.nodeList.keySet()) {
                    Message msg = new Message(Stash.portStr, Integer.toString(Stash.nodeList.get(k))).QueryAll();

                    Log.v("Query *", "sending to " + Stash.nodeList.get(k));

                    Stash.sendMessage(msg);
                }
                Thread.sleep(6000);

                while (Stash.tempflag == false) {
                    Thread.sleep(100);
                }
                return Stash.matcursor;
            }
            if ((!selection.equalsIgnoreCase("*"))
                    && (!selection.equalsIgnoreCase("@"))) {
                Stash.cursor = null;
                Stash.tempflag = false;
                Stash.matcursor = new MatrixCursor(new String[]{"key", "value"});

                String destination = "";


                if (Stash.nodeList.higherEntry(genHash(selection)) != null)
                    destination = Integer.toString(Stash.nodeList.higherEntry(
                            genHash(selection)).getValue());
                else
                    destination = Integer.toString(Stash.nodeList.firstEntry()
                            .getValue());


                Message msg = new Message(Stash.portStr, destination).QuerySelection(selection);

                Log.v("Provider query", "Sending " + selection + " to "
                        + msg.destination);

                Stash.sendMessage(msg);

                destination = "";

                //also ask from replicas
                if (msg.destination.equals("5554")) {
                    destination = "5558";
                } else if (msg.destination.equals("5556")) {
                    destination = "5554";
                } else if (msg.destination.equals("5558")) {
                    destination = "5560";
                } else if (msg.destination.equals("5560")) {
                    destination = "5562";
                } else if (msg.destination.equals("5562")) {
                    destination = "5556";
                }

                Message m1 = new Message(msg.source, destination).QuerySelection(selection);
                Log.d("failure query", "Sending replica query to " + m1.destination);

                Stash.sendMessage(m1);

                while (!Stash.tempflag) {
                    Thread.sleep(100);
                    wait++;
                    if (wait > 10000) {
                        Log.d(Stash.TAG, "Wait exceeded: " + wait);
                        break;
                    }
                }

                wait = 0;
                Thread.sleep(1000);
                Stash.matcursor.moveToFirst();

                Log.d("Query result", "Returning " + Stash.matcursor.getString(Stash.matcursor.getColumnIndex("key")));
                return Stash.matcursor;
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