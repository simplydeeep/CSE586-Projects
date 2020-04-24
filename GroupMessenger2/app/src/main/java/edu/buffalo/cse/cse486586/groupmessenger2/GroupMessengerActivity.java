package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.os.Bundle;
import android.text.method.ScrollingMovementMethod;
import android.view.Menu;
import android.widget.TextView;

/*My code starts*/
import java.net.ServerSocket;
import java.net.Socket;
import java.io.BufferedReader;
import java.io.PrintWriter;
import java.io.InputStreamReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import android.content.Context;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.view.View;
import android.widget.EditText;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.net.Uri;
import android.widget.Button;
/*My code end*/

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 * 
 * @author stevko
 *
 */
public class GroupMessengerActivity extends Activity {

    /*My code starts*/
    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    static final int SERVER_PORT = 10000;
    static int seqNumber = 0; // seqNo (key or name of file) in which the message are stored in content provider

    private String myAvd = null; // avdNo for each AVD
    //private String[] avdArray = new String[5];
    private HashMap<String, Double> msgSeqMap = new HashMap<String, Double>(); // HashMap for message and seqNo
    private HashMap<String, Integer> msgDelMap = new HashMap<String, Integer>(); // HashMap for message and deliverable status (0 or 1)
    private int proSeqNo = 0; // proposedSeqNo sent by each server to the client source AVD
    private Queue<Double> delQ = new LinkedList<Double>(); // Queue for messages which are to be delivered
    private Socket[] socketArr = new Socket[5]; // array storing socket addresses of the 5 AVDs
    private String originalMsg = null; // the message typed in the textbox
    /*My code ends*/

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

        /*My code starts*/
        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        // AVD nos are generated using port numbers (0 to 4)
        myAvd = String.valueOf(((Integer.parseInt(portStr) * 2)-11108)/4);

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return;
        }
        /*My code ends*/

        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        
        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));
        
        /*
         * TODO: You need to register and implement an OnClickListener for the "Send" button.
         * In your implementation you need to get the message from the input box (EditText)
         * and send it to other AVDs.
         */

        /*My code starts*/
        final EditText editText = (EditText) findViewById(R.id.editText1);
        final Button sendB = (Button) findViewById(R.id.button4);
        sendB.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                String msg = editText.getText().toString() + "\n";
                editText.setText(""); // This is one way to reset the input box.
                TextView localTextView = (TextView) findViewById(R.id.textView1);
                localTextView.append("\t" + msg); // This is one way to display a string.

                // new client thread is started as soon as a message is received from the textbox (send button is clicked)
                // this thread uses serial executor to maintain order in which messages are received
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);
            }
        });
        /*My code ends*/
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }

    /*My code starts*/
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            try {
                while (true) {
                    // socket on the server side accepts the connection from client
                    Socket socket = serverSocket.accept();

                    BufferedReader bRD = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

                    // try-catch block is used to catch failure of the AVD while receiving initial msg from client
                    try {
                        // initial msg along with avdNo is received from client source AVD
                        String temp = bRD.readLine();
                        Log.d("ServerTaskStrReceived", temp);

                        if (temp != null) {
                            // the message string is split to separate the message text and avdNo
                            String[] iniMsg = temp.split(":");

                            // if condition used to check whether the message received is the initial message
                            if (!iniMsg[0].equals("F")) {
                                // the textbox message is stored as original Message
                                originalMsg = iniMsg[0];

                                // Initial implementation thought in a way that an array will be maintained for all AVDs
                                // and it'll be used to get the source reference for each message
                                /* Redundant code
                                int avdArrayIndex = Integer.parseInt(iniMsg[1]);
                                Log.e("Server avdArrayIndex", "" + avdArrayIndex);
                                avdArray[avdArrayIndex] = originalMsg;
                                //Log.e("ServerMsgInAvdArray", avdArray[avdArrayIndex]);
                                 */

                                // proposedSeqNo string is prepared by using proposedSeqNo (suppose 10)
                                // and the receiver's AVDNo (suppose 3) which is of the format 10.3 (a double)
                                String iniSeqNo = "" + proSeqNo + "." + myAvd;
                                double iniSeqNoF = Float.parseFloat(iniSeqNo);

                                // The proposedSeqNo appended with avdNo is stored as value in HashMap with originalMsg as key
                                msgSeqMap.put(originalMsg, iniSeqNoF);

                                // At this stage, the agreed seqNo is not received, so the delivery status is marked as 0
                                // and stored in HashMap with originalMsg as key
                                msgDelMap.put(originalMsg, 0);

                                // This is just to check that the stored seqNo in HashMap is as expected, not used further
                                double mapSeqNumberF = msgSeqMap.get(originalMsg);
                                String mapSeqNumber = ""+mapSeqNumberF;
                                Log.e("ServerSeqNoInHashMap", mapSeqNumber);

                                // Code for finalizing the toBeProposedSeqNo by comparing the lastAgreedSeqNo
                                // and lastProposedSeqNo by this AVD.

                                /*
                                double lastAgreedSeqNoF = msgSeqMap.get(originalMsg);
                                double toBeProSeqNoF = lastAgreedSeqNoF > iniSeqNoF ? lastAgreedSeqNoF+1.0 : iniSeqNoF;
                                String toBeProSeqNo = ""+toBeProSeqNoF;

                                Log.e("ServeriniSeqNo", ""+iniSeqNoF);
                                Log.e("ServerlastAgreedSeqNo", ""+lastAgreedSeqNoF);
                                Log.e("ServertoBeProSeqNo", toBeProSeqNo);
                                 */

                                /* The finalized ProposedSeqNo by this AVD is sent to the client which will compare
                                   it with all others received. */
                                // try-catch block is used to catch failure of avd before sending the proposedSeqNo
                                try {
                                    //out.println(toBeProSeqNo);
                                    out.println(iniSeqNoF);
                                } catch (Exception e) {

                                }

                                // this is used to calculate the proposedSeqNo, so it's updated after every message
                                proSeqNo++;
                            }
                        }
                    } catch (Exception e) {

                    }


                    // try-catch block is used to catch failure of the AVD while receiving agreedSeqNo from client
                    try {
                        // The agreedSeqNo string along with the message it's meant for, is received by server of this AVD
                        String temp2 = bRD.readLine();

                        if (temp2 != null) {
                            String[] iniMsg = temp2.split(":");

                            // if condition to check whether this is the final sequence number and not the initial msg
                            if (iniMsg[0].equals("F")) {
                                Log.d("Server", "Ho Ho Ho, here?");

                                // the originalMsg appended with the agreedSeqNo is stored in the string msgF
                                String msgF = iniMsg[2];

                                double finalSeq = Double.parseDouble(iniMsg[1]);

                                // The proposedSeqNo for this message is now replaced with the agreedSeqNo in the HashMap storage
                                msgSeqMap.put(msgF, Double.parseDouble(iniMsg[1]));

                                // Also, the deliverable status of this message is now updated to 1 implying this can be delivered
                                msgDelMap.put(msgF, 1);
                                Log.e("ServrUpdatedSeqNoInHMap", ""+msgSeqMap.get(msgF));

                                // This block of code compares seqNos stored in the HashMap for all messages, transfers them to a list,
                                // and sorts them with the smallest seqNo at the 0th position in the list.

                                // The reference for sorting HashMap entries by values was taken from this link with permission of Professor.
                                // https://www.java67.com/2015/01/how-to-sort-hashmap-in-java-based-on.html
                                Set<Map.Entry<String, Double>> entries = msgSeqMap.entrySet();
                                Comparator<Map.Entry<String, Double>> valueComparator = new Comparator<Map.Entry<String,Double>>() {
                                    @Override public int compare(Map.Entry<String, Double> e1, Map.Entry<String, Double> e2) {
                                        Double v1 = e1.getValue();
                                        Double v2 = e2.getValue();
                                        return v1.compareTo(v2);
                                    }
                                };
                                List<Map.Entry<String, Double>> listOfEntries = new ArrayList<Map.Entry<String, Double>>(entries);
                                Collections.sort(listOfEntries, valueComparator);

                                // The sorted sequence numbers are now added to the delivery queue
                                for(Map.Entry<String, Double> entry : listOfEntries) {
                                    delQ.add(entry.getValue());
                                }

                                /* Now all the seqNos in the queue are checked one by one and their corresponding messages are retrieved
                                   After that, this entry from queue is deleted.
                                   Then the deliverable status of this message is checked.
                                   Those with deliverable status as 1 will be delivered and rest will be discarded */

                                while (!delQ.isEmpty()) {
                                    for (Map.Entry<String, Double> entry : msgSeqMap.entrySet()) {
                                        if (entry.getValue().equals(delQ.peek())) {
                                            String fMsg = entry.getKey();
                                            delQ.remove();
                                            //Double cpSeq = delQ.remove();

                                            /* If the status of message at the head of queue is deliverable, this message is delivered
                                             to the content provider and also removed from both the HashMaps as this is no longer needed */
                                            if (msgDelMap.get(fMsg) == 1) {

                                                msgSeqMap.remove(fMsg);
                                                msgDelMap.remove(fMsg);

                                                /* This block of code puts the message in content provider
                                                   It maintains its own seqNo which are the names of the files
                                                   These seqNos are increased after each successful entry in content provider */
                                                ContentResolver cResolver = getContentResolver();
                                                ContentValues cValues = new ContentValues();
                                                String seq = ""+seqNumber;
                                                cValues.put("key", seq);
                                                //cValues.put("value", iniMsg[2]);
                                                cValues.put("value", fMsg);

                                                Uri.Builder uriBuilder = new Uri.Builder();
                                                uriBuilder.authority("edu.buffalo.cse.cse486586.groupmessenger2.provider");
                                                uriBuilder.scheme("content");
                                                Uri newUri = uriBuilder.build();

                                                cResolver.insert(newUri, cValues);
                                                seqNumber++;
                                            }
                                        }
                                    }
                                }
                                /* This message is also sent to the publishProgress for display at screens of the apps and
                                   after that, connection is closed */
                                publishProgress(originalMsg);
                                socket.close();
                            }
                        }
                    } catch (Exception e) {

                    }
                }
            } catch (IOException e) {
                Log.e(TAG, "ServerTask Exception");
            }
            return null;
        }

        protected void onProgressUpdate(String...strings) {

            //The following code displays on screen the messages which were received in doInBackground().

            String strReceived = strings[0].trim();
            TextView remoteTextView = (TextView) findViewById(R.id.textView1);
            remoteTextView.append(strReceived + "\t\n");
            return;
        }
    }


    /***
     * ClientTask is an AsyncTask that should send a string over the network.
     * It is created by ClientTask.executeOnExecutor() call whenever OnKeyListener.onKey() detects
     * an enter key press event.
     *
     * @author stevko
     *
     */
    private class ClientTask extends AsyncTask<String, Void, Void> {
        double agreedSeqNoF = 0.0;
        String agreedSeqNo = null;
        @Override
        protected Void doInBackground(String... msgs) {
            String[] remotePorts = {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};
            int i = 0;
            for (String remotePort : remotePorts) {
                try {

                    // Client starts connection with the 5 servers.
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remotePort));

                    // All the socket addresses are stored in an array to be used when agreed seqNo will be sent to the servers
                    socketArr[i] = socket;
                    i++;

                    // This is the message received from text box on the app
                    String msgTextBox = msgs[0].trim();
                    Log.e("ClientMsgTextBox", msgTextBox);
                    Log.e("ClientMyAvd", myAvd);

                    // The msg received is appended with the clientAVD number to provide reference to servers
                    // about source of the message so that they can send back the proposed seqNo to this avd
                    String msgToSend = msgTextBox + ":" + myAvd;
                    Log.e("ClientMsgToSend", msgToSend);

                    // Outstream pipe is opened and the msg along with avdNo is sent to the servers
                    // try-catch block is used to catch any exception when the avd fails before sending the msg
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    try {
                        out.println(msgToSend);
                    } catch (Exception e) {

                    }
                    out.flush();

                    BufferedReader bRD = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                    // Through the same connections, client receives proposedSeqNos from all servers
                    // The string is converted to Double for ease of comparison between the proposedseqNos
                    // try-catch block is used to catch any exception when the avd fails while receiving the proposedSeqNos
                    try {
                        String temp = bRD.readLine();
                        if (temp != null) {
                            double tempF = Float.parseFloat(temp);

                            // As the client keeps receiving proposedseqNos, it compares them and stores the max
                            // in agreedSeqNo and discards the rest.
                            if (tempF > agreedSeqNoF) {
                                agreedSeqNoF = tempF;
                            }

                            // agreedSeqNo is appended with 'F' to mark that it's final
                            // and the originalMsg for server's reference
                            agreedSeqNo = "F:" + agreedSeqNoF + ":" + msgTextBox;
                        }
                    } catch (Exception e) {

                    }
                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    Log.e(TAG, "ClientTask socket IOException");
                } catch (Exception e) {
                    Log.e(TAG, "ClientTask Exception");
                }
            }

                /* The socket is kept open so that all communication regarding a single message happens through same channel
                   and FIFO is maintained. After the socket is closed after sending agreedSeqNo, connection for the next message
                   will open up and finished through a single pipe */

                // The agreedSeqNo string is broadcast to all servers using socket addresses from socket array
                // try-catch block is used to catch any exception when the avd fails before sending the agreedSeqNo
                for (int j=0; j<5; j++) {
                try {
                    Log.e(TAG, "Did it reach here?");
                    PrintWriter out = new PrintWriter(socketArr[j].getOutputStream(), true);
                    Log.e("ClientAGREEDSEQNUMBER", agreedSeqNo);
                    try {
                        out.println(agreedSeqNo);
                    } catch (Exception e) {

                    }
                    Log.e("ClientAGREEDSEQNUMBER", "And here? I wonder.");
                    Log.e("ClientSocket", ""+socketArr[j]);
                    // socket is closed once final communication regarding this message is done
                    socketArr[j].close();
                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    Log.e(TAG, "ClientTask socket IOException");
                }
            }
            return null;
        }
    }
    /*My code ends*/
}
