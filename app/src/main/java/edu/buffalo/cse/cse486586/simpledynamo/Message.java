package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Hashtable;

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

    private String sender;
    private String receiver;
    private Stages MessageStage = Stages.NEW;
    private String message;

    private Hashtable<String, Message> RecoveryMessages = new Hashtable<String, Message>();
    private HashMap<String, String> QueryMessages = new HashMap<String, String>();


    public String getSender() {
        return this.sender;
    }

    public String getReceiver() {
        return this.receiver;
    }

    public Stages getMessageStage() {
        return this.MessageStage;
    }

    public String getKey() {
        if (this.message.contains(":")) {
            return this.message.split(":")[0];
        }
        return this.message;
    }

    public String getValue() {
        if (this.message.contains(":")) {
            return this.message.split(":")[1];
        }
        return this.message;
    }

    public Hashtable<String, Message> getRecoveryMessages() {
        return this.RecoveryMessages;
    }

    public HashMap<String, String> getQueryMessages() {
        return this.QueryMessages;
    }

    public Message(String source, String destination) {
        this.sender = source;
        this.receiver = destination;
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
        this.message = key + ":" + value;
        return this;
    }

    public Message QueryAll() {
        this.MessageStage = Stages.QUERY_ALL;
        return this;
    }

    public Message QuerySelection(String selection) {
        this.MessageStage = Stages.QUERY_SEL;
        this.message = selection;
        return this;
    }

    public Message QueryResponse(HashMap<String, String> messages) {
        this.MessageStage = Stages.QUERY_ACK;
        this.QueryMessages = messages;
        return this;
    }
}
