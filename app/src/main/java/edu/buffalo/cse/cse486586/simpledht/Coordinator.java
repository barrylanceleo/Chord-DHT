package edu.buffalo.cse.cse486586.simpledht;

import android.app.Activity;
import android.content.Context;
import android.telephony.TelephonyManager;
import android.util.JsonWriter;
import android.util.Log;
import android.widget.TextView;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by barry on 3/18/16.
 */
public class Coordinator {

    private static final String TAG = Coordinator.class.getSimpleName();
    private final int MY_PORT;
    private final String MY_HASH;
    private static final int INTRODUCER_PORT = 11108;

    private static Coordinator instance;
    private Activity parentActivity;

    Sender mSender;
    BlockingQueue<String> sharedMessageQueue = new LinkedBlockingQueue();

    HashMap<Integer, Socket> socketMap;

    int successor, predecessor;
    String successorHash, predecessorHash;

    private Coordinator(Activity parentActivity) {
        this.parentActivity = parentActivity;

        // initialize my sending port
        TelephonyManager tel = (TelephonyManager) parentActivity.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        MY_PORT = (Integer.parseInt(portStr) * 2);
        MY_HASH = genHash("" + MY_PORT/2);
        displayMessage("MY Port: " + MY_PORT + "\nMY Hash: " +MY_HASH);
        Log.v(TAG, "MY Port: " + MY_PORT + "\nMY Hash: " +MY_HASH);

        // initialize other objects
        socketMap = new HashMap<Integer, Socket>();

        // start a Listener on port 10000
        new Listener(sharedMessageQueue);

        // create a sender
        mSender = new Sender();

        // send a join request to node in avd 5554
        if(MY_PORT != 11108)
        {
            // create a join message
            Message joinMessage = buildMessage(Message.TYPE.JOIN_REQUEST, "", "");
            mSender.sendMessage(buildJsonMessage(joinMessage), INTRODUCER_PORT);
            Log.v(TAG, "JOIN: Sent join message to port: " + INTRODUCER_PORT);

        }

        // initialize its successor and predecessor
        updateSuccessor(MY_PORT);
        updatePredecessor(MY_PORT);

        // start processing the messages in the sharedMessageQueue
        start();

    }

    // this will be called only from the Main Activity
    static Coordinator getInstance(Activity parentActivity) {

        if (instance == null) {
            instance = new Coordinator(parentActivity);
        }
        else{
            // update the new parent activity
            instance.parentActivity = parentActivity;
        }

        return instance;
    }

    static Coordinator getInstance() {
        assert instance != null : TAG + "FATAL ERROR: Trying to create a new Coordinator" +
                " from outside the main activity";
        return instance;
    }

    private String genHash(String input)  {
        MessageDigest sha1 = null;
        try {
            sha1 = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "Exception: No SHA-1 algorithm found", e);
        }
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    void updateSuccessor(int id)
    {

        // clear the previous successor's socket
        if (socketMap.containsKey(successor)){
            try {
                socketMap.get(successor).close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            socketMap.remove(successor);
        }

        successor = id;
        successorHash = genHash("" + id/2);
    }

    void updatePredecessor(int id)
    {
        // clear the previous predecessor's socket
        if (socketMap.containsKey(predecessor)){
            try {
                socketMap.get(predecessor).close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            socketMap.remove(predecessor);
        }

        predecessor = id;
        predecessorHash = genHash("" + id/2);
    }

    Socket getSocket(int port){

        // check if the socket already exists
        if (socketMap.containsKey(port)){
            return socketMap.get(port);
        }

        Socket socket = null;
        try
        {
            socket = new Socket();
            socket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    port));

            // add to the socket map
            socketMap.put(port, socket);
        } catch(Exception e)
        {
            Log.e(TAG, "Exception: Unable to create socket to port: " + port);
        }

        return socket;
    }

    Message buildMessage(Message.TYPE type, String key, String value){

        Message newMessage = new Message();
        newMessage.setType(Message.TYPE.JOIN_REQUEST);
        newMessage.setSourceId(MY_PORT);
        newMessage.setSenderId(MY_PORT);
        newMessage.setKey(key);
        newMessage.setValue(value);

        switch(type){
            case JOIN_REQUEST:
                newMessage.setMessageText("Hello");
                break;

            default:
                Log.e(TAG, "Wrong Message type to build, type:" +type);
                return null;
        }
        return newMessage;
    }

    String buildJsonMessage(Message message)
    {
        StringWriter sWriter = new StringWriter();
        JsonWriter jWriter = new JsonWriter(sWriter);
        String jsonMessage = "";
        try {
            jWriter.beginObject();
            jWriter.name("type").value(message.getTypeId());
            jWriter.name("sourceId").value(message.getSourceId());
            jWriter.name("senderId").value(message.getSenderId());

            jWriter.name("messageText").value(message.getMessageText());

            jWriter.name("key").value(message.getKey());
            jWriter.name("value").value(message.getValue());

            jWriter.endObject();
            jsonMessage = sWriter.toString();

        } catch (IOException e) {
            Log.e(TAG, "Exception: IO Exception in JsonWriter.", e);
        }

        return jsonMessage;
    }

    synchronized Message buildMessageObjectFromJson(String message) throws JSONException{
        JSONObject messageJSON;
        Message mMessage = new Message();
        try {
            messageJSON = new JSONObject(message);
            mMessage.setTypeWithId(messageJSON.getInt("type"));
            mMessage.setSourceId(messageJSON.getInt("sourceId"));
            mMessage.setSenderId(messageJSON.getInt("senderId"));
            mMessage.setMessageText(messageJSON.getString("messageText"));
            mMessage.setKey(messageJSON.getString("key"));
            mMessage.setValue(messageJSON.getString("value"));
        } catch (JSONException e) {
            Log.e(TAG, "Exception: Improper Message Format.");
            throw e;
        } catch (NumberFormatException e) {
            Log.e(TAG, "Exception: Improper Message Format.");
            throw new JSONException("Improper message Format");
        }
        return mMessage;
    }

    void start(){

        while(true){
            try {
                String message = sharedMessageQueue.take();
                Message messageObject = buildMessageObjectFromJson(message);
                Log.v(TAG, "Handling message: " +messageObject);
                handleMessage(messageObject);
            } catch (InterruptedException e) {
                Log.e(TAG, "Exception: Message Consumer was interrupted.", e);
            } catch (JSONException e) {
                Log.e(TAG, "Exception: Improper Message format in  consumer.", e);
            }
        }

    }

    void handleMessage(Message handledMessage)
    {
        int senderId = handledMessage.getSenderId();
        int sourceId = handledMessage.getSourceId();
        switch (handledMessage.getType()){
            case JOIN_REQUEST:
                displayMessage("Received Join Request from " +senderId);
                if(amIResponsible("" + sourceId)){
                    // send predecessor info to joiner and add it as the predecessor
                    String joinResponse = createJoinResponse();
                    handledMessage.setType(Message.TYPE.JOIN_RESPONSE);
                    handledMessage.setSenderId(MY_PORT);
                    handledMessage.setMessageText(joinResponse);
                    mSender.sendMessage(buildJsonMessage(handledMessage), sourceId);

                    Log.v(TAG, "Sent Join Response: " +handledMessage);

                    //TODO need to delete the messages corresponding to the new node

                    updatePredecessor(sourceId);

                    // this is to handle the initial case when the successor is itself
                    if(MY_PORT == successor){
                        updateSuccessor(sourceId);
                    }

                    Log.v(TAG, "JOIN: Added "+ sourceId
                            +"\nMy Predecessor: " +predecessor+" My Successor: " +successor);

                }
                else{
                    // forward the message to the successor
                    handledMessage.setSenderId(MY_PORT);
                    mSender.sendMessage(buildJsonMessage(handledMessage), successor);
                    Log.v(TAG, "Forwarded Join Request to: " + successor + "\n"+ handledMessage);
                }
                break;
            case JOIN_RESPONSE:
                displayMessage("Received Join Response from " +senderId);
                JSONObject messageJSON = null;
                try {
                    messageJSON = new JSONObject(handledMessage.getMessageText());
                    updateSuccessor(messageJSON.getInt("successor"));
                    updatePredecessor(messageJSON.getInt("predecessor"));
                    Log.v(TAG, "JOIN: Added by " + senderId
                            + "\nMy Predecessor: " + predecessor + " My Successor: " + successor);
                } catch (JSONException e) {
                    Log.e(TAG, "Unable tp parse Join Respose: ", e);
                }
                // send JOIN_COMPLETED message to the predecessor to update its successor
                String joinCompletionMessage = createJoinCompletionMessage();
                handledMessage.setType(Message.TYPE.JOIN_COMPLETED);
                handledMessage.setSenderId(MY_PORT);
                handledMessage.setMessageText(joinCompletionMessage);
                mSender.sendMessage(buildJsonMessage(handledMessage), predecessor);

                Log.v(TAG, "Sent Join Completed Message to the predecessor: " + handledMessage);
                break;
            case JOIN_COMPLETED:
                try {
                    messageJSON = new JSONObject(handledMessage.getMessageText());
                    updateSuccessor(messageJSON.getInt("successor"));
                    Log.v(TAG, "JOIN: Updated Successor "
                            + "\nMy Predecessor: " + predecessor + " My Successor: " + successor);
                } catch (JSONException e) {
                    Log.e(TAG, "Unable tp parse Join Respose: ", e);
                }
                break;

        }
    }

    void displayMessage(final String message)
    {
        parentActivity.runOnUiThread(new Runnable() {
            public void run() {
                //display the message
                TextView displayView = (TextView) parentActivity.findViewById(R.id.textView1);
                displayView.append(message +"\n");
            }
        });
    }


    boolean amIResponsible(String key)
    {

        String keyHash = genHash(key);
        if((keyHash.compareTo(predecessorHash) > 0  && keyHash.compareTo(MY_HASH) <= 0) ||
                ((MY_HASH.compareTo(predecessorHash) < 0) &&
                        (keyHash.compareTo(predecessorHash) > 0 || keyHash.compareTo(MY_HASH) <= 0))){
            return true;
        }
        // this is to handle the case when there is only one node in the ring
        if (predecessorHash.equals(MY_HASH)){
            return true;
        }
        return false;
    }

    String createJoinCompletionMessage()
    {
        StringWriter sWriter = new StringWriter();
        JsonWriter jWriter = new JsonWriter(sWriter);
        String joinResponse = "";
        try {
            jWriter.beginObject();
            jWriter.name("successor").value(MY_PORT);

            jWriter.endObject();
            joinResponse = sWriter.toString();

        } catch (IOException e) {
            Log.e(TAG, "Exception: IO Exception in JsonWriter.", e);
        }
        return joinResponse;

    }

    String createJoinResponse(){

        StringWriter sWriter = new StringWriter();
        JsonWriter jWriter = new JsonWriter(sWriter);
        String joinResponse = "";
        try {
            jWriter.beginObject();
            jWriter.name("predecessor").value(predecessor);
            jWriter.name("successor").value(MY_PORT);

            //TODO need to send the key values to be managed by the joiner too

            jWriter.endObject();
            joinResponse = sWriter.toString();

        } catch (IOException e) {
            Log.e(TAG, "Exception: IO Exception in JsonWriter.", e);
        }
        return joinResponse;

    }


}
