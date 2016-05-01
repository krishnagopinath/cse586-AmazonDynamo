package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Hashtable;

import android.database.Cursor;

public class Message implements Serializable {

    private static final long serialVersionUID = 1L;

    enum Stages {

        //Default
        NEW,

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

    String destination, source;
    Stages MessageStage = Stages.NEW;
    String key, value;
    Hashtable<String, Message> RecoveryMessages = new Hashtable<String, Message>();
    HashMap<String, String> QueryMessages = new HashMap<String, String>();


    public Message(String source, String destination) {
        this.source = source;
        this.destination = destination;
    }


    public Message RecoveryRequest() {
        this.MessageStage = Stages.RECOVERY_REQ;
        return this;
    }
    public Message RecoveryResponse(Hashtable<String, Message> messages) {
        this.MessageStage = Stages.RECOVERY_ACK;
        this.RecoveryMessages = messages;
        return this;
    }
    public Message DeleteRequest() {
        this.MessageStage = Stages.DELETE_REQ;
        return this;
    }
    public Message InsertOriginal(String key, String value) {
        this.MessageStage = Stages.INSERT_OR;
        this.key = key;
        this.value = value;
        return this;
    }
    public Message InsertReplica(String key, String value, String rep1, String rep2) {
        this.MessageStage = Stages.INSERT_REP;
        this.key = key;
        this.value = value;
        return this;
    }
    public Message QueryAll() {
        this.MessageStage = Stages.QUERY_ALL;
        this.key = "@";
        return this;
    }
    public Message QueryAllResponse(HashMap<String, String> messages) {
        this.MessageStage = Stages.QUERY_ALL_ACK;
        this.QueryMessages = messages;
        return this;
    }
    public Message QuerySelection(String selection) {
        this.MessageStage = Stages.QUERY_SEL;
        this.key = selection;
        return this;
    }
    public Message QuerySelectionResponse( String key, String value,HashMap<String, String> messages ) {
        this.MessageStage = Stages.QUERY_SEL_ACK;
        this.key = key;
        this.value = value;
        this.QueryMessages = messages;
        return this;
    }


}
