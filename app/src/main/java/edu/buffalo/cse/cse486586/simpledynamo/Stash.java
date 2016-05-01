package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentResolver;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.TreeMap;

/**
 * Created by krishna on 4/30/16.
 */
public class Stash {

    public static SQLiteDatabase sqlite;
    public static Context context;
    public static ContentResolver myContentResolver;
    public static DB mydb;
    public static Cursor cursor = null;
    public static String portStr, pred1, pred2;
    public static String IP = "10.0.2.2";
    static Uri mUri = buildUri("content",
            "edu.buffalo.cse.cse486586.simpledht.provider");
    public static MatrixCursor matcursor;
    static boolean tempflag = false;
    static boolean flag = false;

    static TreeMap<String, Integer> nodeList = new TreeMap<String, Integer>();
    public static HashMap<String, String> hashmap = new HashMap<String, String>();
    public static HashMap<String, String> hashmapstar = new HashMap<String, String>();
    public static Hashtable<String, Message> table = new Hashtable<String, Message>();

    public static String TAG = SimpleDynamoProvider.class.getSimpleName();


    private static Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    public static void sendMessage(Message message) {
        Runnable r = new PackageSender(message);
        Thread t = new Thread(r);
        t.start();
    }
}
