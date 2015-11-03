package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.net.ServerSocket;
import java.net.Socket;
import java.net.InetAddress;

public class SimpleDhtProvider extends ContentProvider {
    private ArrayList<Node> nodeMap;
    private Node headNode;
    MatrixCursor mCursorGlobal = new MatrixCursor(new String[] {"key", "value"});

    public String singleQueryContainer = "";
    public String globalQueryContainer = "";
    public boolean waitingIsOver = false;
    static final String taggeneral = "taggeneral";
    static final String taginsert = "taginsert";
//    static final String REMOTE_PORT0 = "11108";
//    static final String REMOTE_PORT1 = "11112";
//    static final String REMOTE_PORT2 = "11116";
//    static final String REMOTE_PORT3 = "11120";
//    static final String REMOTE_PORT4 = "11124";
    static final String REMOTE_PORT0 = "5554";
    static final String REMOTE_PORT1 = "5556";
    static final String REMOTE_PORT2 = "5558";
    static final String REMOTE_PORT3 = "5560";
    static final String REMOTE_PORT4 = "5562";
    static final String remote_ports[] = {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};
    static final int SERVER_PORT = 10000;
    static final String MSGTYPENOTIF = "MNOTIF";
    static final String MSGTYPEACK = "MACK";
    static final String MSGTYPEGLOBALDEL = "MGD";
    static final String MSGTYPESINGLEDEL = "MSD";
    static final String MSGTYPEINSERTREQUEST = "MINS";

    static final String MSGTYPEGLOBALQUERYREQUEST = "MGQR";
    static final String MSGTYPESINGLEQUERYREQUEST = "MSQR";

    static final String MSGTYPEGLOBALQUERYRESPONSE = "MGQRE";
    static final String MSGTYPESINGLEQUERYRESPONSE = "MSQRE";

    static final String SELECTIONTYPELOCAL = "\"@\"";
    static final String SELECTIONTYPEGLOBAL = "\"*\"";


    public static Uri URI_obj;
    static String ATTR_key;
    static String ATTR_value;
    static String myPort;
    public String predPort;
    public String succPort;
    public String headNodePort;
    public String myNodeHashId;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        if (predPort.equals(succPort) && predPort.equals(myPort) && succPort.equals(myPort)) {
            if ((selection.compareTo(SELECTIONTYPELOCAL) == 0) || (selection.compareTo(SELECTIONTYPEGLOBAL) == 0)) {
                deleteLocalFiles();
            } else {
                getContext().deleteFile(selection);
            }
            return 0;
        }
        if (selection.compareTo(SELECTIONTYPELOCAL) == 0) {
            deleteLocalFiles();
        } else if (selection.compareTo(SELECTIONTYPEGLOBAL) == 0) {
            deleteLocalFiles();
            sendDeleteCommandToSuccessor();
        } else {
            try {
                String hashKey = genHash(selection);
                if (checkIfHeadNode()) {
                    if ((hashKey.compareTo(genHash(predPort)) > 0) || (hashKey.compareTo(myNodeHashId) <= 0)) {
                        getContext().deleteFile(selection);
                    } else {
                        sendSingleDeleteRequestToSuccessor(selection);
                    }
                } else {
                    if (hashKey.compareTo(myNodeHashId) <= 0 && hashKey.compareTo(genHash(predPort)) > 0) {
                        getContext().deleteFile(selection);
                    } else {
                        sendSingleDeleteRequestToSuccessor(selection);
                    }
                }
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }
        return 0;
    }

    private void sendSingleDeleteRequestToSuccessor(String selection) {
        Socket socket = null;
        try {
            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(String.valueOf((Integer.parseInt(succPort) * 2))));
            Log.i(taggeneral, "Sending global delete request to "+succPort);
            MessagePacket msgToSend = generateSingleDeleteRequestMessage(selection);
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())); //write the message received to the output stream
            bw.write(serializeMessage(msgToSend));
            bw.flush();   //force invoke flush to send message and clear buffer.
            socket.close(); //close the socket.
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void sendDeleteCommandToSuccessor() {
        Socket socket = null;
        try {
            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(String.valueOf((Integer.parseInt(succPort) * 2))));
            Log.i(taggeneral, "Sending single delete request to "+succPort);
            MessagePacket msgToSend = generateGlobalDeleteMessage();
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())); //write the message received to the output stream
            bw.write(serializeMessage(msgToSend));
            bw.flush();   //force invoke flush to send message and clear buffer.
            socket.close(); //close the socket.
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void forwardDeleteCommandToSuccessor(MessagePacket msgToSend) {
        Socket socket = null;
        try {
            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(String.valueOf((Integer.parseInt(succPort) * 2))));
            Log.i(taggeneral, "Sending single delete request to "+succPort);
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())); //write the message received to the output stream
            bw.write(serializeMessage(msgToSend));
            bw.flush();   //force invoke flush to send message and clear buffer.
            socket.close(); //close the socket.
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private MessagePacket generateGlobalDeleteMessage() {
        MessagePacket msg = new MessagePacket(SELECTIONTYPEGLOBAL, MSGTYPEGLOBALDEL, myPort, "NULL");
        return msg;
    }

    private MessagePacket generateSingleDeleteRequestMessage(String selection) {
        MessagePacket msg = new MessagePacket(selection, MSGTYPESINGLEDEL, myPort, "NULL");
        return msg;
    }

    public void deleteLocalFiles() {
        String[] localFileList = getContext().fileList();
        for (int i=0;i<localFileList.length;i++) {
            getContext().deleteFile(localFileList[i]);
        }
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        String key = values.getAsString(ATTR_key);
        Log.i(taginsert, "Message generated with key, value = "+key+", "+values.getAsString(ATTR_value)+" at node "+myPort);
        try {
            String hashKey = genHash(key);
//            Log.i("PoRTS", myPort+","+predPort+","+succPort);
            if (predPort.equals(succPort) && predPort.equals(myPort) && succPort.equals(myPort)) {
                Log.i(taggeneral, "Isolated CP Test for "+myPort);
                String filePath = getContext().getFilesDir().getAbsolutePath();
                try {
                    FileWriter fw = new FileWriter(new File(filePath, key));
                    fw.write(values.getAsString(ATTR_value));
                    fw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return uri;
            }
            if (checkIfHeadNode()) {
                if ((hashKey.compareTo(genHash(predPort)) > 0) || (hashKey.compareTo(myNodeHashId) <= 0)) {
                    Log.i(taginsert, "Inserting at head node --> "+key + " with hashkey = "+hashKey);
                    String filePath = getContext().getFilesDir().getAbsolutePath();
                    try {
                        FileWriter fw = new FileWriter(new File(filePath, key));
                        fw.write(values.getAsString(ATTR_value));
                        fw.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    return uri;
                }
            }
            if ((hashKey.compareTo(myNodeHashId) <= 0) && (hashKey.compareTo(genHash(predPort)) > 0)) { //insert in mynode
                Log.i(taginsert, "Inserting at " + myPort + " --> "+key + " with hashkey = "+hashKey);
                String filePath = getContext().getFilesDir().getAbsolutePath();
                try {
                    FileWriter fw = new FileWriter(new File(filePath, key));
                    fw.write(values.getAsString(ATTR_value));
                    fw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return uri;
            } else {
                try {
                    Socket socket = null;
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(String.valueOf((Integer.parseInt(succPort) * 2))));
                    Log.i(taginsert, "Forwarding insert request message to successor Node to "+succPort);
                    MessagePacket msgToSend = generateInsertRequestForwardMessage(values);
                    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())); //write the message received to the output stream
                    bw.write(serializeMessage(msgToSend));
                    bw.flush();   //force invoke flush to send message and clear buffer.
                    socket.close(); //close the socket.
                } catch (UnknownHostException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return null;
    }

    private MessagePacket generateInsertRequestForwardMessage(ContentValues values) {
        MessagePacket msg = new MessagePacket(values.getAsString(ATTR_key)+","+values.getAsString(ATTR_value), MSGTYPEINSERTREQUEST, myPort, "NULL");
        return msg;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        String[] attributes = {ATTR_key, ATTR_value};
        MatrixCursor mCursor = new MatrixCursor(attributes);
        if (predPort.equals(succPort) && predPort.equals(myPort) && succPort.equals(myPort)) {
            if ((selection.compareTo(SELECTIONTYPELOCAL) == 0) || (selection.compareTo(SELECTIONTYPEGLOBAL) == 0)) {
                mCursor = retrieveLocalFiles();
                return mCursor;
            } else {
                try {
                    String dataRead = "";
                    String filePath = getContext().getFilesDir().getAbsolutePath();
                    BufferedReader br = new BufferedReader(new FileReader(new File(filePath, selection)));
                    dataRead = br.readLine();
                    mCursor.addRow(new String[]{selection,dataRead});
                    br.close();
                    return mCursor;
                } catch (Exception ex) {
                    Log.e("File_Query", ex.getMessage()+" -- File read failed");
                }
            }
            return mCursor;
        }
        if (selection.compareTo(SELECTIONTYPELOCAL) == 0) {
            mCursor = retrieveLocalFiles();
        } else if (selection.compareTo(SELECTIONTYPEGLOBAL) == 0) {
            mCursorGlobal = retrieveLocalFiles();
            Cursor tempLocal = mCursorGlobal;
            while(tempLocal.moveToNext()) {
                Log.i(MSGTYPESINGLEQUERYREQUEST, "GLOBAL --- At My Node: "+tempLocal.getString(0)+" Value: "+tempLocal.getString(1));
            }
            sendQueryRequestToSuccessor(selection, MSGTYPEGLOBALQUERYREQUEST);
            try {
                Thread.sleep(1000, 0);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            Log.i("FINALGLOBAL", globalQueryContainer);
            Log.i(MSGTYPESINGLEQUERYREQUEST, "GLOBAL --- Got back message "+mCursorGlobal);
            Cursor temp = mCursorGlobal;
            int i=0;
            while(temp.moveToNext()) {
                Log.i(MSGTYPESINGLEQUERYREQUEST, "GLOBAL --- Got back message with key: "+temp.getString(0)+" Value: "+temp.getString(1));
            }
            Log.i("RETURN", "Returning cursor: "+Arrays.toString(mCursorGlobal.getColumnNames()));
            mCursor = mCursorGlobal;
        } else {
            Log.i("MSG_QUERY", selection+" is being queried for");
            try {
                String hashKey = genHash(selection);
                if (checkIfHeadNode()) {
                    if ((hashKey.compareTo(genHash(predPort)) > 0) || (hashKey.compareTo(myNodeHashId) <= 0)) {
                        String dataRead = "";
                        try {
                            String filePath = getContext().getFilesDir().getAbsolutePath();
                            BufferedReader br = new BufferedReader(new FileReader(new File(filePath, selection)));
                            dataRead = br.readLine();
                            br.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        mCursor.addRow(new String[]{selection,dataRead});
                    } else {
                        sendQueryRequestToSuccessor(selection, MSGTYPESINGLEQUERYREQUEST);
                        waitingIsOver = false;
                        while (waitingIsOver == false) {
                            //wait for message to be transmitted back
                            Thread.sleep(100);
                            Log.i("WAIT1", "Waiting for response");
                        }
                        if (waitingIsOver == true) {
//                            Log.i(MSGTYPESINGLEQUERYREQUEST, "Got back message "+mCursorGlobal);
//                            mCursor = mCursorGlobal;
//                            Cursor temp = mCursorGlobal;
//                            while(temp.moveToNext()) {
//                                Log.i(MSGTYPESINGLEQUERYREQUEST, "Got back message with key: "+temp.getString(0)+" Value: "+temp.getString(1));
//                            }
//                            Log.i("RETURN", "Returning cursor: "+Arrays.toString(mCursorGlobal.getColumnNames()));
//                            return mCursorGlobal;
                            String[] tokens = singleQueryContainer.split("\\,");
                            mCursor.addRow(new String[]{tokens[0],tokens[1]});
                        }
                    }
                } else {
                    if (hashKey.compareTo(myNodeHashId) <= 0 && hashKey.compareTo(genHash(predPort)) > 0) {
                        String dataRead = "";
                        try {
                            String filePath = getContext().getFilesDir().getAbsolutePath();
                            BufferedReader br = new BufferedReader(new FileReader(new File(filePath, selection)));
                            dataRead = br.readLine();
                            br.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        mCursor.addRow(new String[]{selection,dataRead});
                    } else {
                        waitingIsOver = false;
                        sendQueryRequestToSuccessor(selection, MSGTYPESINGLEQUERYREQUEST);
                        while (waitingIsOver == false) {
                            //wait for message to be transmitted back
                            Thread.sleep(100);
                            Log.i("WAIT2", "Waiting for response");
                        }
                        if (waitingIsOver == true) {
//                            Cursor temp = mCursorGlobal;
//                            while(temp.moveToNext()) {
//                                Log.i(MSGTYPESINGLEQUERYREQUEST, "Got back message with key: "+temp.getString(0)+" Value: "+temp.getString(1)+" and mCursorGlobal.size: "+mCursorGlobal.getCount());
//                            }
//                            Log.i("RETURN", "Returning cursor: "+Arrays.toString(mCursorGlobal.getColumnNames()));
//                            return mCursorGlobal;
                            String[] tokens = singleQueryContainer.split("\\,");
                            mCursor.addRow(new String[]{tokens[0],tokens[1]});
                        }
                    }
                }
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
        Log.i("WRONG!", "HERE");
        return mCursor;
    }

    private void sendQueryRequestToSuccessor(String selection, String msgType) {
        try {
            Socket socket = null;
            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(String.valueOf((Integer.parseInt(succPort) * 2))));
            Log.i(taggeneral, "Forwarding query request message to successor Node to "+succPort);
            MessagePacket msgToSend = generateQueryRequestForwardMessage(selection, msgType);
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())); //write the message received to the output stream
            bw.write(serializeMessage(msgToSend));
            bw.flush();   //force invoke flush to send message and clear buffer.
            socket.close(); //close the socket.
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private MessagePacket generateQueryRequestForwardMessage(String selection, String msgType) {
        MessagePacket msg = new MessagePacket(selection, msgType, myPort, "NULL");
        return msg;
    }

    private MatrixCursor retrieveLocalFiles() {
        String[] attributes = {ATTR_key, ATTR_value};
        String messages = "";
        MatrixCursor mCursor = new MatrixCursor(attributes);
        String[] fileList = getContext().fileList();
        String dataRead = "";
        for (int i=0;i<fileList.length;i++) {
            try {
                String filepath = getContext().getFilesDir().getAbsolutePath();
                BufferedReader br = new BufferedReader(new FileReader(new File(filepath, fileList[i])));
                dataRead = br.readLine();
                br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            globalQueryContainer = globalQueryContainer + fileList[i] + "," + dataRead + "*";
            mCursor.addRow(new String[]{fileList[i],dataRead});
        }
        return mCursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean onCreate() {
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
//        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        myPort = portStr;
        predPort = portStr;
        succPort = portStr;
        try {
            myNodeHashId = genHash(myPort);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        nodeMap = new ArrayList<Node>();
        Uri.Builder URI_builder = new Uri.Builder();
        URI_builder.authority("edu.buffalo.cse.cse486586.simpledht.SimpleDhtProvider");
        URI_builder.scheme("content");
        URI_obj = URI_builder.build();
        ATTR_key = new String("key");
        ATTR_value = new String("value");
        Log.i(taggeneral, "MYPORT == "+myPort);
//        if (myPort.equals(String.valueOf((Integer.parseInt(REMOTE_PORT0) * 2)))) {
        if (myPort.equals(REMOTE_PORT0)) {
            Log.i(taggeneral, "Adding master node");
            addMasterNode();
        } else {
            Log.i(taggeneral, "Notifying master node from " + myPort);
            notifyMasterNode();
            ServerSocket serverSocket = null;
            try {
                serverSocket = new ServerSocket(SERVER_PORT);
                new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return false;
    }

    private void addMasterNode() {
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            Log.i(taggeneral, "Inside addMasterNode");
            Node master = new Node();
            master.pred = new Node();
            master.succ = new Node();
            master.hashId = genHash(REMOTE_PORT0);
            master.portNo = REMOTE_PORT0;
            master.pred = master;
            master.succ = master;
            predPort = master.pred.portNo;
            succPort = master.succ.portNo;
//            Log.i(taggeneral, "Master -->"+master.toString());
            nodeMap.add(master);
            headNode = master;
            headNodePort = master.portNo;
        } catch (NoSuchAlgorithmException e) {
            Log.e(taggeneral, "Couldn't add master node");
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void displayNodeMap() {
        String message = "";
        Node temp = new Node();
        for (int i=0;i<nodeMap.size();i++) {
            temp = nodeMap.get(i);
            message += "Node_" + i + ", Hash = "+temp.hashId + ", portNo = "+temp.portNo + " , predPortNo = "+temp.pred.portNo + ", succPortNo = "+temp.succ.portNo + " -----> ";
        }
        Log.i("Nodemap_displayy", "Nodemap ======>> "+message);
        Log.i("headNode_displayy", "HeadNode = "+headNodePort);
    }

    private void notifyMasterNode() {
            MessagePacket msgToSend = generateNotifyMessage();
            new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msgToSend);
    }

    private void addNewNode(String newNodePort) {
        String key = null;
        try {
            key = genHash(newNodePort);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        Node newNode = new Node();
        newNode.hashId = key;
//        newNode.hashId = newNodePort;
        newNode.portNo = newNodePort;
        newNode.pred = new Node();
        newNode.succ = new Node();
        Node currnode = new Node();

        if (nodeMap.size() == 0) {
            newNode.pred = newNode;
            newNode.succ = newNode;
            nodeMap.add(newNode);
            headNode = newNode;
            headNodePort = newNodePort;
        } else if (nodeMap.size() == 1) {
            currnode = nodeMap.get(0);
            if (newNode.hashId.compareTo(currnode.hashId) > 0) {
                newNode.pred = currnode;
                newNode.succ = currnode;
                currnode.pred = newNode;
                currnode.succ = newNode;
                nodeMap.set(0, currnode);
                nodeMap.add(newNode);
            } else if (newNode.hashId.compareTo(currnode.hashId) < 0) {
                newNode.pred = currnode;
                newNode.succ = currnode;
                currnode.pred = newNode;
                currnode.succ = newNode;
                nodeMap.add(0, newNode);
                nodeMap.set(1, currnode);
                headNode = newNode;
                headNodePort = newNodePort;
            }
        } else {
            for (int i=0;i<nodeMap.size();i++) {
                currnode = nodeMap.get(i);
                if (currnode.portNo.equals(headNodePort) && newNode.hashId.compareTo(currnode.hashId) < 0 && newNode.hashId.compareTo(currnode.pred.hashId) < 0) {
                    newNode.succ = currnode;
                    newNode.pred = currnode.pred;
                    nodeMap.get(nodeMap.size() - 1).succ = newNode;
                    currnode.pred = newNode;
                    nodeMap.add(i, newNode);
                    nodeMap.set(i+1, currnode);
                    headNodePort = newNodePort;
                    headNode = newNode;
                } else if (newNode.hashId.compareTo(currnode.hashId) > 0 && newNode.hashId.compareTo(currnode.succ.hashId) > 0 && (i == (nodeMap.size() - 1))) {
                    newNode.succ = currnode.succ;
                    newNode.pred = currnode;
                    currnode.succ = newNode;
                    nodeMap.get(0).pred = newNode;
                    nodeMap.add(newNode);
                    nodeMap.set(i, currnode);
                } else if (newNode.hashId.compareTo(currnode.hashId) > 0 && newNode.hashId.compareTo(currnode.succ.hashId) < 0) {
                    newNode.pred = currnode;
                    newNode.succ = currnode.succ;
                    nodeMap.get(i).succ = newNode;
                    nodeMap.get(i+1).pred = newNode;
                    nodeMap.add(i+1, newNode);
                }

            }
        }
        displayNodeMap();
        sendPredSuccUpdateToNodes(nodeMap);
    }

    private void sendPredSuccUpdateToNodes(ArrayList<Node> nodeMap) {
        Socket socket = null;
        for (int i=0;i<nodeMap.size();i++) {
            try {
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(String.valueOf((Integer.parseInt(nodeMap.get(i).portNo) * 2))));
                Log.i(taggeneral, "Sending Node Port Update to "+nodeMap.get(i).portNo);
                MessagePacket msgToSend = generateAckMessage(nodeMap.get(i));
                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())); //write the message received to the output stream
                bw.write(serializeMessage(msgToSend));
                bw.flush();   //force invoke flush to send message and clear buffer.
                socket.close(); //close the socket.
            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private MessagePacket generateAckMessage(Node node) {
        MessagePacket msg = new MessagePacket(node.pred.portNo+","+node.succ.portNo+","+headNode.portNo+","+node.portNo, MSGTYPEACK, myPort, "NULL");
        return msg;
    }

    private String serializeMessage(MessagePacket m) {
        String selMsg = "";
        selMsg = m.message + "|" + m.msgType + "|" + m.senderPort + "|" + m.selParam;
        return selMsg;
    }

    private MessagePacket unserializeMessage(String msg) {
        if (msg == null) {
            Log.i("unserialize", "null message received");
        } else {
            Log.i("unserialize", "Message = "+msg);
        }
        String[] tokens = msg.split("\\|");
        MessagePacket m;
        m = new MessagePacket(tokens[0], tokens[1], tokens[2], tokens[3]);
        return m;
    }

    private MessagePacket generateNotifyMessage() {
        MessagePacket msg = new MessagePacket("NULL", MSGTYPENOTIF, myPort, "NULL");
        return msg;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        Log.i("SHA-1", "Input = "+input+", Output = "+formatter.toString());
        return formatter.toString();
    }

    private boolean checkIfHeadNode() {
        Log.i("InsideHeadNode" , predPort + " , " + succPort + " , "+myPort);
        if (predPort.equals(succPort) && predPort.equals(myPort) && succPort.equals(myPort)) { //predPort == succPort && predPort == myPort && succPort == myPort
            return false;
        }
        if (myPort.compareTo(headNodePort) == 0) {
            return true;
        } else {
            return false;
        }
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            Socket sock = null;
            String message;
            while (true) {
                try {
                    Log.i(taggeneral, "Inside servertask for " + myPort);
                    sock = serverSocket.accept();
                    BufferedReader br = new BufferedReader(new InputStreamReader(sock.getInputStream()));   //read message from input stream.
                    message = br.readLine();
                    MessagePacket msgPacket = unserializeMessage(message);
                    Log.i(taggeneral, "received message --->"+msgPacket.message+", "+msgPacket.senderPort);
                    if ((msgPacket.msgType.compareTo(MSGTYPENOTIF) == 0) && (myPort.compareTo(REMOTE_PORT0) == 0)) {    //only for master node
                        Log.i(taggeneral, "Message Packet MSGTYPENOTIF received from "+msgPacket.senderPort);
//                        addNewNode(msgPacket.senderPort);
                        addNewNode(msgPacket.senderPort);
                    } else if (msgPacket.msgType.compareTo(MSGTYPEACK) == 0) {
                        String tokens[] = msgPacket.message.split("\\,");
                        predPort = tokens[0];
                        succPort = tokens[1];
                        headNodePort = tokens[2];
                        Log.i(taggeneral, "tokens = "+ Arrays.toString(tokens));
                        Log.i(taggeneral, "Master ACKED node added, pred  = "+predPort+", succ = "+succPort+", headNode = "+headNodePort);
//                        displayNodeMap();
                    } else if (msgPacket.msgType.compareTo(MSGTYPEINSERTREQUEST) == 0) {
                        if (myPort.equals(msgPacket.senderPort)) {
                            Log.i(taggeneral, "Message is LOST!!! --- >"+ msgPacket.message);
                        } else {
                            String tokens[] = msgPacket.message.split("\\,");
                            String msgKey = tokens[0];
                            String msgValue = tokens[1];
                            try {
                                String hashKey = genHash(msgKey);
                                if (checkIfHeadNode()) {
                                    Log.i(taginsert, "At HeadNode, msgHash = "+hashKey + ", MyNodeHash = "+myNodeHashId + ", PredHash = "+predPort);
                                    if ((hashKey.compareTo(genHash(predPort)) > 0) || (hashKey.compareTo(myNodeHashId) <= 0)) {
                                        //INSERT AT MY NODE
                                        String filePath = getContext().getFilesDir().getAbsolutePath();
                                        Log.i(taginsert, "Inserting at headnode " + myPort + " --> "+msgKey + " with hashkey = "+hashKey);
                                        try {
                                            FileWriter fw = new FileWriter(new File(filePath, msgKey));
                                            fw.write(msgValue);
                                            fw.close();
                                        } catch (IOException e) {
                                            e.printStackTrace();
                                        }
                                    } else {
                                        forwardInsertRequestToSuccessor(msgPacket);
                                    }
                                } else {
                                    if (hashKey.compareTo(myNodeHashId) <= 0 && hashKey.compareTo(genHash(predPort)) > 0) {
                                        //INSERT AT MY NODE
                                        Log.i(taginsert, "Inserting at " + myPort + " --> "+msgKey + " with hashkey = "+hashKey);
                                        String filePath = getContext().getFilesDir().getAbsolutePath();
                                        try {
                                            FileWriter fw = new FileWriter(new File(filePath, msgKey));
                                            fw.write(msgValue);
                                            fw.close();
                                        } catch (IOException e) {
                                            e.printStackTrace();
                                        }
                                    } else {
                                        forwardInsertRequestToSuccessor(msgPacket);
                                    }
                                }
                            } catch (NoSuchAlgorithmException e) {
                                e.printStackTrace();
                            }
                        }
                    } else if (msgPacket.msgType.compareTo(MSGTYPEGLOBALDEL) == 0) {
                        if (msgPacket.senderPort.equals(myPort)) {
                            //do nothing, message has circulated back to me
                        } else {
                            deleteLocalFiles();
                            forwardDeleteCommandToSuccessor(msgPacket);
                        }
                    } else if (msgPacket.msgType.compareTo(MSGTYPESINGLEDEL) == 0) {
                        try {
                            String hashKey = genHash(msgPacket.message);
                            if (checkIfHeadNode()) {
                                if ((hashKey.compareTo(genHash(predPort)) > 0) || (hashKey.compareTo(myNodeHashId) <= 0)) {
                                    getContext().deleteFile(msgPacket.message);
                                } else {
                                    forwardDeleteCommandToSuccessor(msgPacket);
                                }
                            } else {
                                if (hashKey.compareTo(myNodeHashId) <= 0 && hashKey.compareTo(genHash(predPort)) > 0) {
                                    getContext().deleteFile(msgPacket.message);
                                } else {
                                    forwardDeleteCommandToSuccessor(msgPacket);
                                }
                            }
                        } catch (NoSuchAlgorithmException e) {
                            e.printStackTrace();
                        }
                    } else if (msgPacket.msgType.compareTo(MSGTYPESINGLEQUERYREQUEST) == 0) {
                        Log.i(MSGTYPESINGLEQUERYREQUEST, "Received Query Request for "+msgPacket.message+" from "+msgPacket.senderPort);
                        if (myPort.equals(msgPacket.senderPort)) {  //Query request travels back to sender node
                            Log.i(taggeneral, "Message is NOT FOUND!!! --- >"+ msgPacket.message);
                            waitingIsOver = true;
                        } else {
                            try {
                                String hashKey = genHash(msgPacket.message);
                                if (checkIfHeadNode()) {
                                    if ((hashKey.compareTo(genHash(predPort)) > 0) || (hashKey.compareTo(myNodeHashId) <= 0)) {
                                        String dataRead = "";
                                        try {
                                            String filePath = getContext().getFilesDir().getAbsolutePath();
                                            BufferedReader brq = new BufferedReader(new FileReader(new File(filePath, msgPacket.message)));
                                            dataRead = brq.readLine();
                                            sendSingleQueryDataReadToRequester(dataRead, msgPacket);
                                            brq.close();
                                        } catch (IOException e) {
                                            e.printStackTrace();
                                        }
                                    } else {
                                        forwardQueryCommandToSuccessor(msgPacket);
                                    }
                                } else {
                                    if (hashKey.compareTo(myNodeHashId) <= 0 && hashKey.compareTo(genHash(predPort)) > 0) {
                                        String dataRead = "";
                                        try {
                                            String filePath = getContext().getFilesDir().getAbsolutePath();
                                            BufferedReader brq = new BufferedReader(new FileReader(new File(filePath, msgPacket.message)));
                                            dataRead = brq.readLine();
                                            sendSingleQueryDataReadToRequester(dataRead, msgPacket);
                                            brq.close();
                                        } catch (IOException e) {
                                            e.printStackTrace();
                                        }
                                    } else {
                                        forwardQueryCommandToSuccessor(msgPacket);
                                    }
                                }
                            } catch (NoSuchAlgorithmException e) {
                                e.printStackTrace();
                            }
                        }
                    } else if (msgPacket.msgType.compareTo(MSGTYPESINGLEQUERYRESPONSE) == 0) {
                        String[] tokens = msgPacket.message.split("\\,");
                        Log.i(MSGTYPESINGLEQUERYRESPONSE, "Got Queried message back with key: "+tokens[0]+", value: "+tokens[1]+" from "+msgPacket.senderPort);
                        singleQueryContainer = msgPacket.message;
//                        mCursorGlobal.addRow(new String[]{tokens[0], tokens[1]});
                        waitingIsOver = true;
                    } else if (msgPacket.msgType.compareTo(MSGTYPEGLOBALQUERYREQUEST) == 0) {
                        if (msgPacket.senderPort.equals(myPort)) {
                            //all messages received
                            Log.i(MSGTYPEGLOBALQUERYREQUEST, "ALL DUMPS DONE!!! Back to base!");
                        } else {
                            Log.i(MSGTYPEGLOBALQUERYREQUEST, "Global Dump requested by "+msgPacket.senderPort);
                            String dataRead = "";
                            String allMessages = "";
                            String[] localFileList = getContext().fileList();
                            for (int i=0;i<localFileList.length;i++) {
                                try {
                                    String filePath = getContext().getFilesDir().getAbsolutePath();
                                    BufferedReader brq = new BufferedReader(new FileReader(new File(filePath, localFileList[i])));
                                    dataRead = brq.readLine();
                                    allMessages = allMessages + localFileList[i]+","+dataRead+"*";
                                    brq.close();
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }
                            sendAllMyMessagesToRequester(allMessages, msgPacket, localFileList.length);
                            forwardQueryCommandToSuccessor(msgPacket);
                        }
                    } else if (msgPacket.msgType.compareTo(MSGTYPEGLOBALQUERYRESPONSE) == 0) {
                        Log.i("COUNTOFMSG", "Received Query Response from "+msgPacket.senderPort+" and messageCount = "+msgPacket.selParam);
                        Log.i(MSGTYPEGLOBALQUERYRESPONSE, "Messages = "+msgPacket.message);
                        globalQueryContainer = globalQueryContainer + msgPacket.message;
                        String[] msgList = msgPacket.message.split("\\*");
                        for (int i=0; i<msgList.length;i++) {
                            if (msgList[i] == null) {
                                Log.i("NULLMSG", "NULL message do nothing");
                                //do nothing
                            } else {
                                String tokens[] = msgList[i].split("\\,");
                                if (tokens.length == 1) {
                                    Log.i("SINGLETOKEN", "SINGLETOKEN do nothing");
                                    //d0 nothing
                                } else {
                                    mCursorGlobal.addRow(new String[]{tokens[0], tokens[1]});
                                }
                            }
                        }
                    }
                    displayNodeMap();
                } catch (IOException err) {
                    Log.e("ServerTask", "Server failed");
                }
            }
        }
    }

    private void sendAllMyMessagesToRequester(String allMessages, MessagePacket msgPacket, int length) {
        try {
            Log.i(taginsert, "Sending at " + myPort + " --> "+msgPacket.message+" to "+succPort);
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(String.valueOf((Integer.parseInt(msgPacket.senderPort) * 2))));
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())); //write the message received to the output stream
            MessagePacket msgToSend = generateQueryResultNotifyMessageForGlobalRequest(allMessages, length);
            bw.write(serializeMessage(msgToSend));
            bw.flush();   //force invoke flush to send message and clear buffer.
            socket.close(); //close the socket.
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void forwardInsertRequestToSuccessor(MessagePacket msgPacket) {
        try {
            Log.i(taginsert, "Forwarding at " + myPort + " --> "+msgPacket.message+" to "+succPort);
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(String.valueOf((Integer.parseInt(succPort) * 2))));
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())); //write the message received to the output stream
            bw.write(serializeMessage(msgPacket));
            bw.flush();   //force invoke flush to send message and clear buffer.
            socket.close(); //close the socket.
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void forwardQueryCommandToSuccessor(MessagePacket msgPacket) {
        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(String.valueOf((Integer.parseInt(succPort) * 2))));
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())); //write the message received to the output stream
            bw.write(serializeMessage(msgPacket));
            bw.flush();   //force invoke flush to send message and clear buffer.
            socket.close(); //close the socket.
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void sendSingleQueryDataReadToRequester(String dataRead, MessagePacket msgPacket) {
        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(String.valueOf((Integer.parseInt(msgPacket.senderPort) * 2))));
            MessagePacket msgToSend = generateQueryResultNotifyMessageForSingleRequest(dataRead, msgPacket);
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())); //write the message received to the output stream
            Log.i("MSQR", "Sending back "+serializeMessage(msgToSend)+" to port: "+msgPacket.senderPort);
            bw.write(serializeMessage(msgToSend));
            bw.flush();   //force invoke flush to send message and clear buffer.
            socket.close(); //close the socket.
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private MessagePacket generateQueryResultNotifyMessageForSingleRequest(String dataRead, MessagePacket msgPacket) {
        MessagePacket msg = new MessagePacket(msgPacket.message+","+dataRead, MSGTYPESINGLEQUERYRESPONSE, myPort, "NULL");
        return msg;
    }

    private MessagePacket generateQueryResultNotifyMessageForGlobalRequest(String dataRead, int length) {
        MessagePacket msg = new MessagePacket(dataRead, MSGTYPEGLOBALQUERYRESPONSE, myPort, String.valueOf(length));
        return msg;
    }

    private class ClientTask extends AsyncTask<MessagePacket, Void, Void> {
        protected Void doInBackground(MessagePacket... msgs) {
            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(String.valueOf((Integer.parseInt(REMOTE_PORT0) * 2))));
                MessagePacket msgToSend = msgs[0];
                Log.i(taggeneral, "After socket creation, sending "+msgToSend.msgType+" message to "+REMOTE_PORT0+ " with Port no = "+String.valueOf((Integer.parseInt(REMOTE_PORT0) * 2)));
                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())); //write the message received to the output stream
                bw.write(serializeMessage(msgToSend));
                bw.flush();   //force invoke flush to send message and clear buffer.
                socket.close(); //close the socket.
            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }
    }
}





