package edu.buffalo.cse.cse486586.simpledynamo;

import java.net.ServerSocket;

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
            if (Stash.nodeList.higherEntry(Stash.genHash(key)) != null)
                destination = Integer.toString(Stash.nodeList.higherEntry(
                        Stash.genHash(key)).getValue());
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
        }
        return null;
    }

    @Override
    public boolean onCreate() {
        try {
            Stash.mydb = new DB(getContext());
            Stash.sqlite = Stash.mydb.getWritableDatabase();
            Stash.context = getContext();

            //Stash.store = Stash.context.getSharedPreferences(Stash.PREFS_NAME, 0);


            Stash.myContentResolver = Stash.context.getContentResolver();
            TelephonyManager tel = (TelephonyManager) Stash.context
                    .getSystemService(Context.TELEPHONY_SERVICE);
            Stash.portStr = tel.getLine1Number().substring(
                    tel.getLine1Number().length() - 4);


            //generate hashes and keep in nodelist for comparisons (TreeMap is useful for such things)
            Stash.nodeList.put(Stash.genHash(Integer.toString(5554)), 5554);
            Stash.nodeList.put(Stash.genHash(Integer.toString(5556)), 5556);
            Stash.nodeList.put(Stash.genHash(Integer.toString(5558)), 5558);
            Stash.nodeList.put(Stash.genHash(Integer.toString(5560)), 5560);
            Stash.nodeList.put(Stash.genHash(Integer.toString(5562)), 5562);


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
                    Message msg = new Message(Stash.portStr, Integer.toString(Stash.nodeList.get(k))).RecoveryRequest();
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


                if (Stash.nodeList.higherEntry(Stash.genHash(selection)) != null)
                    destination = Integer.toString(Stash.nodeList.higherEntry(
                            Stash.genHash(selection)).getValue());
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


}