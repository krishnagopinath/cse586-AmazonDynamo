package edu.buffalo.cse.cse486586.simpledynamo;

import java.net.ServerSocket;
import java.util.LinkedList;
import java.util.Map;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.telephony.TelephonyManager;
import android.util.Log;


/*NodeList : 5562 --> 5556 --> 5554 --> 5558 --> 5560*/
public class SimpleDynamoProvider extends ContentProvider {

    double wait = 0;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {

        Stash.sqlite.delete("Msg", null, null);

        //Stash.store.edit().clear().commit();

        for (String k : Stash.nodeList.keySet()) {
            Message msg = new Message(Stash.portStr, Stash.nodeList.get(k)).DeleteRequest();
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
            if (Stash.nodeList.higherEntry(Stash.genHash(key)) != null)
                destination = Stash.nodeList.higherEntry(Stash.genHash(key)).getValue();
            else
                destination = Stash.nodeList.firstEntry().getValue();


            Log.v("Insert provider", " Destination is " + destination);


            Message m = new Message(Stash.portStr, destination).InsertOriginal(key, value);

            Stash.table.put(m.key, m);


            //insert it in destination first
            Stash.sendMessage(m, Thread.MAX_PRIORITY);


            //find replicas for destination

            String[] replicas = Stash.successorMap.get(m.destination);

            Message m2 = new Message(Stash.portStr, replicas[0]).InsertReplica(m.key, m.value, replicas[0], replicas[1]);
            Message m3 = new Message(Stash.portStr, replicas[1]).InsertReplica(m.key, m.value, replicas[0], replicas[1]);

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
        }
        return null;
    }

    @Override
    public boolean onCreate() {
        try {
            Stash.context = getContext();

            Stash.sqlite = new DB(Stash.context).getWritableDatabase();
            Stash.store = Stash.context.getSharedPreferences(Stash.PREFS_NAME, 0);


            TelephonyManager tel = (TelephonyManager) Stash.context
                    .getSystemService(Context.TELEPHONY_SERVICE);
            Stash.portStr = tel.getLine1Number().substring(
                    tel.getLine1Number().length() - 4);

            /*NodeList : 5562 --> 5556 --> 5554 --> 5558 --> 5560*/
            //set up ring structure for later use
            LinkedList<String> nodeRing = new LinkedList<String>();
            nodeRing.add("5562");
            nodeRing.add("5556");
            nodeRing.add("5554");
            nodeRing.add("5558");
            nodeRing.add("5560");


            //generate hashes and keep in nodelist for comparisons (TreeMap is useful for such things)
            for (int i = 0; i < Stash.PORTS.length; i++) {
                Stash.nodeList.put(Stash.genHash(Stash.PORTS[i]), Stash.PORTS[i]);
            }

            //maintain predecessors of nodes for easy access later
            Stash.predecessorMap.put("5554", new String[]{"5556", "5562"});
            Stash.predecessorMap.put("5556", new String[]{"5562", "5560"});
            Stash.predecessorMap.put("5558", new String[]{"5554", "5556"});
            Stash.predecessorMap.put("5560", new String[]{"5558", "5554"});
            Stash.predecessorMap.put("5562", new String[]{"5560", "5558"});


            //also maintain successors to save us from redundancy later
            Stash.successorMap.put("5554", new String[]{"5558", "5560"});
            Stash.successorMap.put("5556", new String[]{"5554", "5558"});
            Stash.successorMap.put("5558", new String[]{"5560", "5562"});
            Stash.successorMap.put("5560", new String[]{"5562", "5556"});
            Stash.successorMap.put("5562", new String[]{"5556", "5554"});


            ServerSocket serverSocket = new ServerSocket(10000);
            new Thread(new PackageReceiver(serverSocket)).start();
            Thread.sleep(1000);

            synchronized (Stash.sqlite) {
                //ask for messages
                for (String k : Stash.nodeList.keySet()) {
                    Message msg = new Message(Stash.portStr, Stash.nodeList.get(k)).RecoveryRequest();
                    Log.v("Recovery Query *", "sending to " + Stash.nodeList.get(k));
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
            // Thread.sleep(7000);

            Log.i(Stash.TAG, "Query key: " + selection);
            if (selection.equals("@")) {
                Stash.table.clear();
                Thread.sleep(6000);

                return Stash.sqlite.query("Msg", null, null, null, null, null, null);

                /*
                Map<String, ?> rows = Stash.store.getAll();

                MatrixCursor cursor = new MatrixCursor(new String[]{Stash.KEY_FIELD, Stash.VALUE_FIELD});

                for (String key : rows.keySet()) {
                    cursor.addRow(new Object[]{key, rows.get(key).toString()});
                }

                return cursor;
                */


            }
            if (selection.equals("*")) {
                Stash.cursor = null;
                Stash.tempflag = false;
                Stash.matcursor = new MatrixCursor(new String[]{"key", "value"});
                for (String k : Stash.nodeList.keySet()) {
                    Message msg = new Message(Stash.portStr, Stash.nodeList.get(k)).QueryAll();

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


                if (Stash.nodeList.higherEntry(Stash.genHash(selection)) != null)
                    destination = Stash.nodeList.higherEntry(Stash.genHash(selection)).getValue();
                else
                    destination = Stash.nodeList.firstEntry().getValue();


                Message msg = new Message(Stash.portStr, destination).QuerySelection(selection);

                Log.v("Provider query", "Sending " + selection + " to "
                        + msg.destination);

                Stash.sendMessage(msg);


                //also ask from replicas
                destination = Stash.successorMap.get(msg.destination)[0];


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


}