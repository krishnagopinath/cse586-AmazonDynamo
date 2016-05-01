package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.content.SharedPreferences;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.TreeMap;

/**
 * Created by krishna on 4/30/16.
 */

public class Stash {


    public static Context context;
    public static SQLiteDatabase sqlite;




    public static final String PREFS_NAME = "KeyValueStore";
    public static final String KEY_FIELD = "key";
    public static final String VALUE_FIELD = "value";
    public static final String[] PORTS = new String[]{"5554", "5556", "5558", "5560", "5562"};

    public static void sendMessage(Message message) {
        Thread t = new Thread(new PackageSender(message));
        t.start();
    }

    public static void sendMessage(Message message, Integer priority) {
        Thread t = new Thread(new PackageSender(message));
        t.setPriority(priority);
        t.start();
    }


    public static String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        @SuppressWarnings("resource")
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    public static String getPosition(String selection) {
        String position = "";
        try {
            if (Stash.nodeHashMap.higherEntry(Stash.genHash(selection)) != null)
                position = Stash.nodeHashMap.higherEntry(Stash.genHash(selection)).getValue();
            else
                position = Stash.nodeHashMap.firstEntry().getValue();
        } catch (NoSuchAlgorithmException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return position;
    }

    public static String portStr;


    public static TreeMap<String, String> nodeHashMap = new TreeMap<String, String>();
    public static HashMap<String, String[]> predecessorMap = new HashMap<String, String[]>();
    public static HashMap<String, String[]> successorMap = new HashMap<String, String[]>();

    public static Hashtable<String, Message> RecoveryMessages = new Hashtable<String, Message>();

    static boolean waitFlagger = false;

    public static SharedPreferences store;

    public static MatrixCursor matrixCursor;


}
