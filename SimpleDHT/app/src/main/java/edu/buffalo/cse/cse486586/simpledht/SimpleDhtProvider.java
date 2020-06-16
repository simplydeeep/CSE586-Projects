package edu.buffalo.cse.cse486586.simpledht;

import java.net.ServerSocket;
import java.net.Socket;
import java.net.InetAddress;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.MatrixCursor;
import android.database.Cursor;
import android.net.Uri;

import android.util.Log;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.IOException;

import java.util.HashMap;
import java.util.Map;

public class SimpleDhtProvider extends ContentProvider {

    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    static final int SERVER_PORT = 10000;

    private final String fNode = "5554";
    private final String Join = "J";
    private final String Update = "U";
    private final String Insert = "I";
    private final String Delete = "D";
    private final String DeleteAllGlobal = "DG";
    private final String Query = "Q";
    private final String QueryAllGlobal = "QG";

    private String mNode;
    private String pNode = null;
    private String sNode = null;
    private String qResponse;
    private String gResponse;
    private boolean flag = true;
    private HashMap<String, String> localMap = new HashMap<String, String>();

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        String key1 = (String) values.get("key");
        String value1 = (String) values.get("value");
        String inMessage = key1 + ":" + value1;
        insert(inMessage);
        return uri;
    }

    public void insert(String message) {
        String fName = message.split(":")[0];
        String fContent = message.split(":")[1];
        String hName;

        /* Logcat basis code */
        try {
            String keyHash = genHash(fName);
            String nodeHash = genHash(mNode);
            String succHash;
            if (sNode != null) {
                succHash = genHash(sNode);
                Log.w("My Insert", "My Node: "+mNode+nodeHash+" succ Node: "+ sNode+succHash +" keyHash: "+fName);
            }
            else Log.w("My Insert", "My Node: "+mNode+nodeHash +" keyHash: "+fName);
        } catch (NoSuchAlgorithmException e) { }

        try {
            hName = genHash(fName);
            if ((pNode == null && sNode == null)
                    || (hName.compareTo(genHash(pNode)) > 0 && hName.compareTo(genHash(mNode)) <= 0)
                    || (genHash(pNode).compareTo(genHash(mNode)) > 0 && (hName.compareTo(genHash(pNode)) > 0
                    || hName.compareTo(genHash(mNode)) <= 0))) {
                localMap.put(fName, fContent);
            } else {
                String iMsg = Insert+":"+sNode+":"+message;
                Log.w("Insert Succ", iMsg);
                clientThread(iMsg);
            }
        } catch (NoSuchAlgorithmException e) { Log.e("Insert", "Hashing problem"); }
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        String port = mNode;
        if (selection.equals("*")) {
            String g = QueryAllGlobal+":"+port+":"+port+":"+"T";
            return queryAllGlobal(g);
        }
        else if (selection.equals("@")) {
            return queryAllLocal();
        }
        else {
            String q = Query+":"+port+":"+selection+":"+null+":"+port+":"+"T";
            return query(q);
        }
    }

    public Cursor query (String message) {
        String port = message.split(":")[4];
        String selection = message.split(":")[2];

        MatrixCursor matCursor = new MatrixCursor(new String[]{"key", "value"});

        try {
            String hName = genHash(selection);
            if ((pNode == null && sNode == null)
                    || (hName.compareTo(genHash(pNode)) > 0 && hName.compareTo(genHash(mNode)) <= 0)
                    || (genHash(pNode).compareTo(genHash(mNode)) > 0 && (hName.compareTo(genHash(pNode)) > 0
                    || hName.compareTo(genHash(mNode)) <= 0))) {

                if (port.equals(mNode)) {
                    MatrixCursor.RowBuilder mRB = matCursor.newRow();
                    mRB.add("key", selection);
                    mRB.add("value", localMap.get(selection));
                } else {
                    String qReply = Query + ":" + port + ":" + selection + ":" + localMap.get(selection)+ ":"+ port+":"+"T";
                    clientThread(qReply);
                }
                return matCursor;
            } else {
                String qMsg = Query + ":" + sNode + ":" + selection+":"+null+":"+port+":"+"T";
                clientThread(qMsg);
                if (message.split(":")[5].equals("T")) {
                    while (flag) {}
                    MatrixCursor.RowBuilder mRB = matCursor.newRow();
                    mRB.add("key", selection);
                    mRB.add("value", qResponse);
                    qResponse = null;
                    flag = true;
                }
                return matCursor;
            }

        } catch (NoSuchAlgorithmException e) { Log.d("Query", "Hashing problem"); }
        return matCursor;
    }

    public Cursor queryAllLocal() {
        MatrixCursor matCursor = new MatrixCursor(new String[]{"key", "value"});

        for (Map.Entry<String, String> entries: localMap.entrySet()) {
            MatrixCursor.RowBuilder mRB = matCursor.newRow();
            mRB.add("key", entries.getKey());
            mRB.add("value", entries.getValue());
        }
        return matCursor;
    }

    public Cursor queryAllGlobal(String message) {
        MatrixCursor matCursor = new MatrixCursor(new String[]{"key", "value"});

        if (pNode == null && sNode == null) {
            for (Map.Entry<String, String> entries : localMap.entrySet()) {
                MatrixCursor.RowBuilder mRB = matCursor.newRow();
                mRB.add("key", entries.getKey());
                mRB.add("value", entries.getValue());
            }
            return matCursor;
        }

        else {
            for (Map.Entry<String, String> entries : localMap.entrySet()) {
                gResponse += ":"+entries.getKey()+"__"+entries.getValue();
            }
            String gMsg = message.substring(0,3) + sNode + message.substring(7)+":"+gResponse;
            clientThread(gMsg);

            if (message.split(":")[3].equals("T")) {
                while (flag) { }
                String[] fullResponse = gResponse.split(":");
                int i=0;
                while (i<fullResponse.length) {
                    if (!fullResponse[i].equals("null")) {
                        String key1 = fullResponse[i].split("__")[0];
                        String val1 = fullResponse[i].split("__")[1];
                        MatrixCursor.RowBuilder mRB = matCursor.newRow();
                        mRB.add("key", key1);
                        mRB.add("value", val1);
                    }
                    i++;
                }
                gResponse = null;
                flag = true;
            }
        }
        return matCursor;
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        String port = mNode;
        if (selection.equals("*")) {
            String d = DeleteAllGlobal+":"+port+":"+port;
            deleteAllGlobal(d);
        }
        else if (selection.equals("@")) {
            deleteAllLocal();
        }
        else {
            String d = Delete+":"+port+":"+selection+":"+port;
            delete(d);
        }
        return 0;
    }

    public void delete (String message) {
        String port = message.split(":")[3];
        String selection = message.split(":")[2];
        try {
            String hName = genHash(selection);
            if ((pNode == null && sNode == null)
                    || (hName.compareTo(genHash(pNode)) > 0 && hName.compareTo(genHash(mNode)) <= 0)
                    || (genHash(pNode).compareTo(genHash(mNode)) > 0 && (hName.compareTo(genHash(pNode)) > 0
                    || hName.compareTo(genHash(mNode)) <= 0))) {
                localMap.remove(selection);
            } else {
                String dMsg = Delete + ":" + sNode + ":" + selection+":"+port;
                clientThread(dMsg);
            }
        } catch (NoSuchAlgorithmException e) { Log.e("Query", "Hashing problem"); }
    }

    public void deleteAllLocal() { localMap.clear(); }

    public void deleteAllGlobal(String message) {
        localMap.clear();
        String port = message.split(":")[2];
        if (!sNode.equals(port)) {
            String gMsg = DeleteAllGlobal+":"+sNode+":"+port;
            clientThread(gMsg);
        }
    }

    @Override
    public boolean onCreate() {
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        mNode = portStr;

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return false;
        }
        if (!mNode.equals(fNode)) {
            String msg = Join+":"+fNode+":"+mNode;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);
        }
        return true;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            try {
                while (true) {
                    Socket socket = serverSocket.accept();
                    BufferedReader bRD = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String temp = bRD.readLine();
                    Log.w("ServerStr", temp+" &&& Req received by: "+mNode+" pNode: "+pNode+" sNode: "+sNode);

                    if (temp != null) {
                        String[] msgReceived = temp.split(":");

                        if (msgReceived[0].equals(Join)) {

                            if (pNode == null && sNode == null) {
                                sNode = msgReceived[2];
                                pNode = msgReceived[2];
                                Log.w("FirstNodeJoin", "mNode: " + mNode + " pNode: " + pNode + " sNode: " + sNode);

                                String updateP = Update + ":" +msgReceived[2] + ":" + msgReceived[1] + ":" + msgReceived[1];
                                clientThread(updateP);

                            } else {
                                try {
                                    String hashedKey = genHash(msgReceived[2]);
                                    if ((genHash(pNode).compareTo(genHash(msgReceived[1])) > 0 && hashedKey.compareTo(genHash(pNode)) > 0)
                                            || (genHash(pNode).compareTo(genHash(msgReceived[1])) > 0 && hashedKey.compareTo(genHash(msgReceived[1])) <= 0)
                                            || (hashedKey.compareTo(genHash(pNode)) > 0 && hashedKey.compareTo(genHash(msgReceived[1])) <= 0)) {

                                        String updateS = Update + ":" + pNode + ":" + null + ":" + msgReceived[2];
                                        clientThread(updateS);

                                        String update = Update + ":" + msgReceived[2] + ":" + pNode + ":" + msgReceived[1];
                                        clientThread(update);

                                        pNode = msgReceived[2];
                                        Log.w("OtherNodesJoin", "Node: " + mNode + " pNode: " + pNode + " sNode: " + sNode);

                                    } else {
                                        String msg = Join + ":" + sNode + ":" + msgReceived[2];
                                        clientThread(msg);
                                    }
                                } catch (NoSuchAlgorithmException e) { }
                            }
                        }
                        else if (msgReceived[0].equals(Update)) {
                            if (!msgReceived[2].equals("null")) {
                                pNode = msgReceived[2];
                            }
                            if (!msgReceived[3].equals("null")) {
                                sNode = msgReceived[3];
                            }
                            Log.w("Updated", "Node: " + mNode + " pNode: " + pNode + " sNode: " + sNode);
                        }
                        else if (msgReceived[0].equals(Insert)) {
                            Log.w("Insert Called", "My Node: "+mNode);
                            insert(msgReceived[2]+":"+msgReceived[3]);
                        }
                        else if (msgReceived[0].equals(Query)) {
                            Log.e("Server Query", "My Node: "+mNode+ " Port: "+msgReceived[4]);
                            if (mNode.equals(msgReceived[4])) {
                                qResponse = msgReceived[3];
                                Log.e("Server Query Original", "My Node: "+mNode+ " qResponse: "+qResponse);
                                flag = false;
                            } else {
                                String q = msgReceived[0]+":"+msgReceived[1]+":"+msgReceived[2]+":"+msgReceived[3]+":"+msgReceived[4]+":"+"F";
                                Log.e("Server Query Else ", "My Node: "+mNode+ " Message: "+q);
                                query(q);
                            }
                        }
                        else if (msgReceived[0].equals(QueryAllGlobal)) {
                            Log.e("Server Global", "My Node: "+mNode+ " Port: "+msgReceived[2]);
                            if (mNode.equals(msgReceived[2])) {
                                gResponse = temp.substring(15);
                                Log.e("Server Global Original", "My Node: "+mNode+ " gResponse: "+gResponse);
                                flag = false;
                            } else {
                                String g = temp.substring(0,13) + "F" + temp.substring(14);
                                Log.e("Server Global Else ", "My Node: "+mNode+ " Message: "+g);
                                queryAllGlobal(g);
                            }
                        }
                        else if (msgReceived[0].equals(Delete)) {
                            delete(temp);
                        }
                        else if (msgReceived[0].equals(DeleteAllGlobal)) {
                            deleteAllGlobal(temp);
                        }
                    }
                    socket.close();
                }
            } catch (IOException e) { Log.e(TAG, "ServerTask Exception"); }
            return null;
        }

        protected void onProgressUpdate(String...strings) { }
    }

    public void clientThread(String message) {
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {
            String[] msgReceived = msgs[0].split(":");
            int port = (Integer.parseInt(msgReceived[1]))*2;
            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), port);
                String msgToSend = msgs[0];
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                out.println(msgToSend);
                out.flush();
                socket.close();

            } catch (Exception e) {
                Log.e(TAG, "ClientTask Exception");
            }
            return null;
        }
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }
}
