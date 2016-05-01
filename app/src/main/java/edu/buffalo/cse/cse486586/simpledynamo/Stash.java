package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentResolver;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.TreeMap;

/**
 * Created by krishna on 4/30/16.
 */

public class Stash {



    public static Context context;
    public static ContentResolver myContentResolver;

    public static SQLiteDatabase sqlite;

    public static DB mydb;


    public static Cursor cursor = null;

    public static String portStr;

    public static MatrixCursor matcursor;
    static boolean tempflag = false;

    public static HashMap<String, String> hashmap = new HashMap<String, String>();
    public static HashMap<String, String> hashmapstar = new HashMap<String, String>();
    public static Hashtable<String, Message> table = new Hashtable<String, Message>();

    public static final String TAG = SimpleDynamoProvider.class.getSimpleName();
    public static final String PREFS_NAME = "KeyValueStore";

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

    public static TreeMap<String, Integer> nodeList = new TreeMap<String, Integer>();
    public static HashMap<String, String[]> predecessorMap = new HashMap<String, String[]>();
    public static HashMap<String, String[]> successorMap = new HashMap<String, String[]>();

    public static SharedPreferences store;



}
