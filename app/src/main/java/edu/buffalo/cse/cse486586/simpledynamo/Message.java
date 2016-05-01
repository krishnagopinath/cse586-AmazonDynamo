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

    enum Stages {

        //Recovery
        RECOVERY_REQ,
        RECOVERY_ACK,

        //Delete
        DELETE_REQ,

        //Insert
        INSERT_OR,
        INSERT_REP,

        //Query
        QUERY_ALL,
        QUERY_SEL,
        QUERY_ALL_ACK,
        QUERY_SEL_ACK

    }

    public Message(String source, String destination) {
        this.source = source;
        this.destination = destination;
    }


    public Message RecoveryRequest() {

        this.type = "tableReq";
        this.key = "@";
        this.Querytype = "all";


        return this;
    }

    public Message RecoveryResponse(Hashtable<String, Message> messages) {
        this.type = "table";
        this.hashtable.putAll(messages);

        return this;
    }

    public Message DeleteRequest() {
        this.type = "delete";
        this.Querytype = "all";

        return this;
    }

    public Message InsertOriginal(String key, String value) {

        this.key = key;
        this.value = value;
        this.type = "insert";
        this.state = "original";
        this.originalID = destination;


        return this;
    }

    public Message InsertReplica(String key, String value, String rep1, String rep2) {
        this.originalID = this.destination;
        this.key = key;
        this.value = value;
        this.type = "insert";
        this.rep1 = rep1;
        this.rep2 = rep2;
        this.state = "replica";


        return this;
    }

    public Message QueryAll() {


        this.type = "query";
        this.key = "@";
        this.Querytype = "all";


        return this;
    }

    public Message QueryAllResponse(HashMap<String, String> messages) {


        this.hashmap = messages;
        this.type = "cursor";
        this.Querytype = "all";


        return this;
    }

    public Message QuerySelection(String selection) {


        this.type = "query";
        this.key = selection;

        return this;
    }

    public Message QuerySelectionReplica( String selection) {

        this.key = selection;
        this.type = "query";


        return this;
    }

    public Message QuerySelectionResponse( String key, String value,HashMap<String, String> messages ) {


        this.key = key;
        this.value = value;
        this.hashmap = messages;
        this.type = "cursor";

        return this;
    }


}
