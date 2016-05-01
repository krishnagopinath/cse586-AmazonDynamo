package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Hashtable;

import android.database.Cursor;

public class Message implements Serializable {

    private static final long serialVersionUID = 1L;
    String type, subtype;
    String key, value;
    String destination, source, rep1, rep2;
    String Querytype = "basic";
    String state, originalID;
    HashMap<String, String> hashmap = new HashMap<String, String>();
    Hashtable<String, Message> hashtable = new Hashtable<String, Message>();


    public Message RecoveryRequest(String source, String destination) {

        this.source = source;
        this.destination = destination;
        this.type = "tableReq";
        this.key = "@";
        this.Querytype = "all";


        return this;
    }

    public Message RecoveryResponse(String source, String destination, Hashtable<String, Message> messages) {
        this.source = source;
        this.destination = destination;
        this.type = "table";
        this.hashtable.putAll(messages);

        return this;
    }

    public Message DeleteRequest(String source, String destination) {
        this.source = source;
        this.destination = destination;
        this.type = "delete";
        this.Querytype = "all";

        return this;
    }

    public Message InsertOriginal(String source, String destination, String key, String value) {


        this.source = source;
        this.destination = destination;
        this.key = key;
        this.value = value;
        this.type = "insert";
        this.state = "original";
        this.originalID = destination;


        return this;
    }

    public Message InsertReplica(String source, String destination, String key, String value, String rep1, String rep2) {

        this.source = source;
        this.originalID = destination;
        this.destination = destination;
        this.key = key;
        this.value = value;
        this.type = "insert";
        this.rep1 = rep1;
        this.rep2 = rep2;
        this.state = "replica";


        return this;
    }

    public Message QueryAll(String source, String destination) {


        this.source = source;
        this.destination = destination;
        this.type = "query";
        this.key = "@";
        this.Querytype = "all";


        return this;
    }

    public Message QueryAllResponse(String source, String destination, HashMap<String, String> messages) {



        this.source = source;
        this.destination = destination;
        this.hashmap = messages;
        this.type = "cursor";
        this.Querytype = "all";


        return this;
    }

    public Message QuerySelection(String source, String destination, String selection) {


        this.source = source;
        this.destination = destination;
        this.type = "query";
        this.key = selection;

        return this;
    }

    public Message QuerySelectionReplica(String source, String destination, String selection) {

        this.source = source;
        this.destination = destination;
        this.key = selection;
        this.type = "query";


        return this;
    }

    public Message QuerySelectionResponse(String source, String destination, String key, String value,HashMap<String, String> messages ) {

        this.source = source;
        this.destination = destination;
        this.key = key;
        this.value = value;
        this.hashmap = messages;
        this.type = "cursor";

        return this;
    }


}
