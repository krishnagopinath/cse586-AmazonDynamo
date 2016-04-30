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
            Message msg = new Message();
            msg.type = "delete";
            msg.Querytype = "all";
            Log.v("Delete *", "sending to " + Stash.nodeList.get(k));
            msg.destination = Integer.toString(Stash.nodeList.get(k));
            Runnable r = new PackageSender(msg);
            Thread th = new Thread(r);
            th.start();
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

            String k = values.getAsString("key");
            String v = values.getAsString("value");
            // int position = getcorrectNodeId(genHash(k));
            Message m = new Message();
            m.key = k;
            m.value = v;
            //find right location
            if (Stash.nodeList.higherEntry(genHash(k)) != null)
                m.destination = Integer.toString(Stash.nodeList.higherEntry(
                        genHash(k)).getValue());
            else
                m.destination = Integer.toString(Stash.nodeList.firstEntry()
                        .getValue());


            Log.v("Insert provider", " Destination is " + m.destination);
            m.type = "insert";
            m.state = "original";
            m.originalID = m.destination;
            Stash.table.put(m.key, m);


            //insert it in destination first
            Runnable r = new PackageSender(m);
            Thread th = new Thread(r);
            th.setPriority(Thread.MAX_PRIORITY);
            th.start();

            //check destination and find replicas for destination
            if (m.destination.equalsIgnoreCase("5554")) {
                Message m2 = new Message();
                m2.key = m.key;
                m2.value = m.value;
                m2.type = m.type;
                m2.rep1 = "5560";
                m2.rep2 = "5558";
                m2.originalID = m.destination;
                m2.destination = "5560";
                m2.state = "replica";

                //send to first replica
                Runnable r2 = new PackageSender(m2);
                Thread th2 = new Thread(r2);
                Log.v("Replication ", "Sending " + m2.key + "  to "
                        + m.destination);
                th2.setPriority(Thread.MAX_PRIORITY);
                th2.start();

                Message m3 = new Message();
                m3.key = m.key;
                m3.value = m.value;
                m3.type = m.type;
                m3.originalID = m.destination;
                m3.destination = "5558";
                m3.rep1 = "5560";
                m3.rep2 = "5558";
                m3.state = "replica";
                Log.v("Replication ", "Sending " + m3.key + "  to "
                        + m.destination);

                //send to second replica
                Runnable r3 = new PackageSender(m3);
                Thread th3 = new Thread(r3);
                th3.setPriority(Thread.MAX_PRIORITY);
                th3.start();

            } else if (m.destination.equalsIgnoreCase("5556")) {

                Message m2 = new Message();
                m2.key = m.key;
                m2.value = m.value;
                m2.type = m.type;
                m2.originalID = m.destination;
                m2.destination = "5558";
                m2.state = "replica";
                m2.rep1 = "5554";
                m2.rep2 = "5558";
                Log.v("Replication ", "Sending " + m2.key + "  to "
                        + m.destination);
                Runnable r2 = new PackageSender(m2);
                Thread th2 = new Thread(r2);
                th2.setPriority(Thread.MAX_PRIORITY);
                th2.start();
                Message m3 = new Message();
                m3.key = m.key;
                m3.value = m.value;
                m3.type = m.type;
                m3.state = "replica";
                m3.originalID = m.destination;
                m3.destination = "5554";
                m3.rep1 = "5554";
                m3.rep2 = "5558";
                Log.v("Replication ", "Sending " + m3.key + "  to "
                        + m.destination);
                Runnable r3 = new PackageSender(m3);
                Thread th3 = new Thread(r3);
                th3.setPriority(Thread.MAX_PRIORITY);
                th3.start();

            } else if (m.destination.equalsIgnoreCase("5558")) {

                Message m2 = new Message();
                m2.key = m.key;
                m2.value = m.value;
                m2.type = m.type;
                m2.state = "replica";
                m2.originalID = m.destination;
                m2.destination = "5560";
                m2.rep1 = "5560";
                m2.rep2 = "5562";
                Log.v("Replication ", "Sending " + m2.key + "  to "
                        + m.destination);
                Runnable r2 = new PackageSender(m2);
                Thread th2 = new Thread(r2);
                th2.setPriority(Thread.MAX_PRIORITY);
                th2.start();
                Message m3 = new Message();
                m3.key = m.key;
                m3.value = m.value;
                m3.type = m.type;
                m3.state = "replica";
                m3.originalID = m.destination;
                m3.destination = "5562";
                m3.rep1 = "5562";
                m3.rep2 = "5560";
                Log.v("Replication ", "Sending to " + m.destination);
                Runnable r3 = new PackageSender(m3);
                Thread th3 = new Thread(r3);
                th3.setPriority(Thread.MAX_PRIORITY);
                th3.start();

            } else if (m.destination.equalsIgnoreCase("5560")) {

                Message m2 = new Message();
                m2.key = m.key;
                m2.value = m.value;
                m2.type = m.type;
                m2.state = "replica";
                m2.originalID = m.destination;
                m2.destination = "5562";
                m2.rep1 = "5562";
                m2.rep2 = "5556";
                Log.v("Replication ", "Sending to " + m.destination);
                Runnable r2 = new PackageSender(m2);
                Thread th2 = new Thread(r2);
                th2.setPriority(Thread.MAX_PRIORITY);
                th2.start();
                Message m3 = new Message();
                m3.key = m.key;
                m3.value = m.value;
                m3.type = m.type;
                m3.state = "replica";
                m3.originalID = m.destination;
                m3.destination = "5556";
                m3.rep1 = "5556";
                m3.rep2 = "5562";
                Log.v("Replication ", "Sending to " + m.destination);
                Runnable r3 = new PackageSender(m3);
                Thread th3 = new Thread(r3);
                th3.setPriority(Thread.MAX_PRIORITY);
                th3.start();

            } else if (m.destination.equalsIgnoreCase("5562")) {

                Message m2 = new Message();
                m2.key = m.key;
                m2.value = m.value;
                m2.type = m.type;
                m2.state = "replica";
                m2.originalID = m.destination;
                m2.destination = "5554";
                m2.rep1 = "5556";
                m2.rep2 = "5554";
                Log.v("Replication ", "Sending " + m2.key + "  to "
                        + m.destination);
                Runnable r2 = new PackageSender(m2);
                Thread th2 = new Thread(r2);
                th2.setPriority(Thread.MAX_PRIORITY);
                th2.start();
                Message m3 = new Message();
                m3.key = m.key;
                m3.value = m.value;
                m3.type = m.type;
                m3.state = "replica";
                m3.originalID = m.destination;
                m3.destination = "5556";
                m3.rep1 = "5556";
                m3.rep2 = "5554";
                Log.v("Replication ", "Sending to " + m.destination);
                Runnable r3 = new PackageSender(m3);
                Thread th3 = new Thread(r3);
                th3.setPriority(Thread.MAX_PRIORITY);
                th3.start();

            }

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
            Runnable r = new PackageReceiver(serverSocket);
            Thread the = new Thread(r);
            the.start();
            Thread.sleep(1000);
            synchronized (Stash.sqlite) {
                //ask for messages
                for (String k : Stash.nodeList.keySet()) {
                    Message msg = new Message();
                    msg.type = "tableReq";
                    msg.key = "@";
                    msg.Querytype = "all";
                    msg.source = Stash.portStr;
                    Log.v("Recovery Query *",
                            "sending to " + Stash.nodeList.get(k));
                    msg.destination = Integer.toString(Stash.nodeList.get(k));
                    Runnable ro = new PackageSender(msg);
                    Thread th = new Thread(ro);
                    th.setPriority(Thread.MIN_PRIORITY);

                    th.start();
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
                    Message msg = new Message();
                    msg.type = "query";
                    msg.key = "@";
                    msg.Querytype = "all";
                    msg.source = Stash.portStr;
                    Log.v("Query *", "sending to " + Stash.nodeList.get(k));
                    msg.destination = Integer.toString(Stash.nodeList.get(k));
                    Runnable r = new PackageSender(msg);
                    Thread th = new Thread(r);
                    th.start();
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
                Message msg = new Message();
                msg.type = "query";
                msg.key = selection;

                if (Stash.nodeList.higherEntry(genHash(msg.key)) != null)
                    msg.destination = Integer.toString(Stash.nodeList.higherEntry(
                            genHash(msg.key)).getValue());
                else
                    msg.destination = Integer.toString(Stash.nodeList.firstEntry()
                            .getValue());

                //ask for query from source
                msg.source = Stash.portStr;
                Log.v("Provider query", "Sending " + selection + " to "
                        + msg.destination);
                Runnable r = new PackageSender(msg);
                Thread th = new Thread(r);
                th.start();

                //also ask from replicas
                if (msg.destination.equals("5554")) {
                    Message m1 = new Message();
                    m1.key = msg.key;
                    m1.type = msg.type;
                    m1.source = msg.source;
                    m1.destination = "5558";
                    Log.d("failure query", "Sending replica query to " + m1.destination);
                    Runnable ro = new PackageSender(m1);
                    Thread tho = new Thread(ro);
                    tho.start();
                } else if (msg.destination.equals("5556")) {

                    Message m1 = new Message();
                    m1.key = msg.key;
                    m1.type = msg.type;
                    m1.source = msg.source;
                    m1.destination = "5554";
                    Log.d("failure query", "Sending replica query to " + m1.destination);
                    Runnable ro = new PackageSender(m1);
                    Thread tho = new Thread(ro);
                    tho.start();
                } else if (msg.destination.equals("5558")) {

                    Message m1 = new Message();
                    m1.key = msg.key;
                    m1.type = msg.type;
                    m1.source = msg.source;
                    m1.destination = "5560";
                    Log.d("failure query", "Sending replica query to " + m1.destination);
                    Runnable ro = new PackageSender(m1);
                    Thread tho = new Thread(ro);
                    tho.start();
                } else if (msg.destination.equals("5560")) {
                    Message m1 = new Message();
                    m1.key = msg.key;
                    m1.type = msg.type;
                    m1.source = msg.source;
                    m1.destination = "5562";
                    Log.d("failure query", "Sending replica query to " + m1.destination);
                    Runnable ro = new PackageSender(m1);
                    Thread tho = new Thread(ro);
                    tho.start();
                } else if (msg.destination.equals("5562")) {

                    Message m1 = new Message();
                    m1.key = msg.key;
                    m1.type = msg.type;
                    m1.source = msg.source;
                    m1.destination = "5556";
                    Log.d("failure query", "Sending replica query to " + m1.destination);
                    Runnable ro = new PackageSender(m1);
                    Thread tho = new Thread(ro);
                    tho.start();
                }
                while (Stash.tempflag == false) {
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