package edu.buffalo.cse.cse486586.simpledht;

/**
 * Created by saptarshi on 3/27/15.
 */
public class MessagePacket {
    public String message;
    public String msgType;
    public String senderPort;
    public String selParam;

    public MessagePacket(String message, String msgType, String senderPort, String selParam) {
        this.message = message;
        this.msgType = msgType;
        this.senderPort = senderPort;
        this.selParam = selParam;
    }
}


