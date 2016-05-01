package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Hashtable;

import android.database.Cursor;

public class Message implements Serializable {

    enum Stages {

        //Default
        NEW,

        //Recovery
        RECOVERY_REQ,
        RECOVERY_ACK,

        //Delete
        DELETE_REQ,

        //Insert
        INSERT_REQ,

        //Query
        QUERY_ALL,
        QUERY_SEL,
        QUERY_ACK

    }

    private String destination, source;
    private Stages MessageStage = Stages.NEW;
    private String key, value;
    private Hashtable<String, Message> RecoveryMessages = new Hashtable<String, Message>();
    private HashMap<String, String> QueryMessages = new HashMap<String, String>();


    public String getSource() {
        return this.source;
    }


    public String getDestination() {
        return this.destination;
    }

    public Stages getMessageStage() {
        return this.MessageStage;
    }

    public String getKey() {
        return this.key;
    }

    public String getValue() {
        return this.value;
    }

    public Hashtable<String, Message> getRecoveryMessages() {
        return this.RecoveryMessages;
    }

    public HashMap<String, String> getQueryMessages() {
        return this.QueryMessages;
    }

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

    public Message InsertRequest(String key, String value) {
        this.MessageStage = Stages.INSERT_REQ;
        this.key = key;
        this.value = value;
        return this;
    }

    public Message QueryAll() {
        this.MessageStage = Stages.QUERY_ALL;
        this.key = "@";
        return this;
    }


    public Message QuerySelection(String selection) {
        this.MessageStage = Stages.QUERY_SEL;
        this.key = selection;
        return this;
    }

    public Message QueryResponse(HashMap<String, String> messages) {
        this.MessageStage = Stages.QUERY_ACK;
        this.QueryMessages = messages;
        return this;
    }


}
