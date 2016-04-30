package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Hashtable;

import android.database.Cursor;

public class Message implements Serializable{

	private static final long serialVersionUID = 1L;
	String type, subtype;
	String key, value;
	String destination, source, rep1, rep2;
	String Querytype="basic";
	String state, originalID;
	HashMap<String, String> hashmap=new HashMap<String, String>();
	Hashtable<String, Message> hashtable = new Hashtable<String, Message>();

	
	
	

}
