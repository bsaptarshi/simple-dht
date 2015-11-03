package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentValues;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by saptarshi on 3/27/15.
 */
public class Node {
    public String hashId;
    public Node pred;
    public Node succ;
    public String portNo;

    public Node() {
        this.pred = null;
        this.succ = null;
        this.hashId = "";
        this.portNo = "";
    }
}
