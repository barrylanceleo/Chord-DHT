package edu.buffalo.cse.cse486586.simpledht;

import android.app.Activity;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.telephony.TelephonyManager;
import android.util.JsonWriter;
import android.util.Log;
import android.widget.TextView;

import org.json.JSONArray;
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
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by barry on 3/18/16.
 */
public class Coordinator {

    private static final String TAG = Coordinator.class.getSimpleName();
    final int MY_PORT;
    private final String MY_HASH;
    private static final int INTRODUCER_PORT = 11108;

    private static Coordinator instance;
    private Activity parentActivity;


    Sender mSender;
    BlockingQueue<String> sharedMessageQueue = new LinkedBlockingQueue();

    HashMap<Integer, Socket> socketMap;

    int successor, predecessor;
    String successorHash, predecessorHash;

    private final ReentrantLock joinLock = new ReentrantLock();
    private int joinCompletedResponseCount = 0;

    Lock insertLock = new ReentrantLock();
    Condition insertCondVar = insertLock.newCondition();
    String insertResponse = "FAILED";


    Lock queryLock = new ReentrantLock();
    Condition queryCondVar = queryLock.newCondition();
    String queryResponse = "FAILED";

    Lock deleteLock = new ReentrantLock();
    Condition deleteCondVar = deleteLock.newCondition();
    String deleteResponse = "FAILED";

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
        if(instance == null){
            Log.e(TAG,  "FATAL ERROR: Trying to create a new Coordinator" +
                    " from outside the main activity");
        }
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
        newMessage.setType(type);
        newMessage.setSourceId(MY_PORT);
        newMessage.setSenderId(MY_PORT);
        newMessage.setKey(key);
        newMessage.setValue(value);

        switch(type){
            case JOIN_REQUEST:
                newMessage.setMessageText("Hello");
                break;
            case INSERT:
                newMessage.setMessageText("Inserting");
                break;
            case QUERY:
                newMessage.setMessageText("Querying");
                break;
            case DELETE:
                newMessage.setMessageText("Deleting");
                break;
            default:
                Log.v(TAG, "Building message, type:" +type);
                return newMessage;
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
            jWriter.close();
            jsonMessage = sWriter.toString();

        } catch (IOException e) {
            Log.e(TAG, "Exception: IO Exception in JsonWriter.", e);
        }

        return jsonMessage;
    }

    Message buildMessageObjectFromJson(String message) throws JSONException{
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
        String key = handledMessage.getKey();
        String value = handledMessage.getValue();
        switch (handledMessage.getType()){
            case JOIN_REQUEST:
                displayMessage("Received Join Request from " +senderId);

                // if a join is already in progress put the join request back in the queue and proceed.
                if(joinLock.isLocked()){
                    try {
                        sharedMessageQueue.put(buildJsonMessage(handledMessage));
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    break;
                }

                // acquire the join lock and proceed
                joinLock.lock();

                if(amIResponsible("" + sourceId/2)){
                    // forward the request to myself
                    handledMessage.setType(Message.TYPE.JOIN_REQUEST_FORWARD);
                    handledMessage.setSenderId(MY_PORT);
                    mSender.sendMessage(buildJsonMessage(handledMessage), MY_PORT);
                    Log.v(TAG, "Forwarded Join Request to: " + successor + "\n" + handledMessage);
                }
                else{
                    // forward the message to the successor
                    handledMessage.setType(Message.TYPE.JOIN_REQUEST_FORWARD);
                    handledMessage.setSenderId(MY_PORT);
                    mSender.sendMessage(buildJsonMessage(handledMessage), successor);
                    Log.v(TAG, "Forwarded Join Request to: " + successor + "\n"+ handledMessage);
                }
                break;
            case JOIN_REQUEST_FORWARD:
                if(amIResponsible("" + sourceId/2)){
                    // send predecessor info to joiner and add it as the predecessor
                    String joinResponse = createJoinResponse();
                    handledMessage.setType(Message.TYPE.JOIN_RESPONSE);
                    handledMessage.setSenderId(MY_PORT);
                    handledMessage.setMessageText(joinResponse);
                    mSender.sendMessage(buildJsonMessage(handledMessage), sourceId);

                    Log.v(TAG, "Sent Join Response: " +handledMessage);

                    //TODO need to delete the messages corresponding to the new node

                    updatePredecessor(sourceId);

//                    // this is to handle the initial case when the successor is itself
//                    if(MY_PORT == successor){
//                        updateSuccessor(sourceId);
//                    }

                    Log.v(TAG, "JOIN: Added "+ sourceId
                            +"\nMy Predecessor: " +predecessor+" My Successor: " +successor);

                    displayMessage("JOIN: Added "+ sourceId
                            +"\nMy Predecessor: " +predecessor+" My Successor: " +successor);

                    // send join completed message to INTRODUCER
                    handledMessage.setType(Message.TYPE.JOIN_COMPLETED);
                    handledMessage.setSenderId(MY_PORT);
                    handledMessage.setMessageText("COMPLETED");
                    mSender.sendMessage(buildJsonMessage(handledMessage), INTRODUCER_PORT);
                    Log.v(TAG, "JOIN: Sent Completion Message to: " + INTRODUCER_PORT);

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

                    displayMessage("JOIN: Added by " + senderId
                            + "\nMy Predecessor: " + predecessor + " My Successor: " + successor);

                } catch (JSONException e) {
                    Log.e(TAG, "Unable tp parse Join Respose: ", e);
                }
                // send JOIN_INFO message to the predecessor to update its successor
                String joinInfoMessage = createJoinInfoMessage();
                handledMessage.setType(Message.TYPE.JOIN_INFO);
                handledMessage.setSenderId(MY_PORT);
                handledMessage.setMessageText(joinInfoMessage);
                mSender.sendMessage(buildJsonMessage(handledMessage), predecessor);

                Log.v(TAG, "Sent Join INFO Message to the predecessor to " +
                        "update its successor: " + handledMessage);

                // send join completed message to INTRODUCER
                handledMessage.setType(Message.TYPE.JOIN_COMPLETED);
                handledMessage.setSenderId(MY_PORT);
                handledMessage.setMessageText("COMPLETED");
                mSender.sendMessage(buildJsonMessage(handledMessage), INTRODUCER_PORT);
                Log.v(TAG, "JOIN: Sent Completion Message to: " + INTRODUCER_PORT);

                break;
            case JOIN_INFO:
                try {
                    messageJSON = new JSONObject(handledMessage.getMessageText());
                    updateSuccessor(messageJSON.getInt("successor"));
                    Log.v(TAG, "JOIN: Updated Successor "
                            + "\nMy Predecessor: " + predecessor + " My Successor: " + successor);
                    displayMessage("JOIN: Updated Successor "
                            + "\nMy Predecessor: " + predecessor + " My Successor: " + successor);
                } catch (JSONException e) {
                    Log.e(TAG, "Unable to parse Join Response: ", e);
                }

                // send join completed message to INTRODUCER
                handledMessage.setType(Message.TYPE.JOIN_COMPLETED);
                handledMessage.setSenderId(MY_PORT);
                handledMessage.setMessageText("COMPLETED");
                mSender.sendMessage(buildJsonMessage(handledMessage), INTRODUCER_PORT);
                Log.v(TAG, "JOIN: Sent Completion Message to: " + INTRODUCER_PORT);

                break;
            case JOIN_COMPLETED:
                ++joinCompletedResponseCount;
                Log.v(TAG, "JOIN: Received Join Completed Message number: " +joinCompletedResponseCount);
                if(joinCompletedResponseCount == 3)
                {
                    joinLock.unlock();
                    joinCompletedResponseCount = 0;
                    Log.v(TAG, "JOIN: Received 3 Join Completed Messages. Released join lock");
                }
                break;

            case INSERT:

                if(amIResponsible(key)){
                    // insert message in my content provider
                    ContentResolver mContentResolver = parentActivity.getContentResolver();
                    ContentValues cv = new ContentValues();
                    cv.put("key", key);
                    cv.put("value", value);
                    mContentResolver.insert(SimpleDhtProvider.CPUri, cv);
                    Log.v(TAG, "INSERT: Stored message in Content Provider.");

                    // send insert completion message to source id
                    handledMessage.setType(Message.TYPE.INSERT_COMPLETED);
                    handledMessage.setSenderId(MY_PORT);
                    mSender.sendMessage(buildJsonMessage(handledMessage), handledMessage.getSourceId());
                    Log.v(TAG, "INSERT: Sent Insert completed message to: " + handledMessage.getSourceId());

                }
                else{
                    // forward message to succesor
                    handledMessage.setSenderId(MY_PORT);
                    handledMessage.setMessageText("COMPLETED");
                    mSender.sendMessage(buildJsonMessage(handledMessage), successor);
                    Log.v(TAG, "INSERT: Forwarded Insert message to: " + successor);
                }
                break;

            case INSERT_COMPLETED:
                // update the response
                insertResponse = "Inserted at " +handledMessage.getSenderId();
                // notify the condition variable
                insertLock.lock();
                insertCondVar.signal();
                insertLock.unlock();
                break;
            case QUERY:

                if(amIResponsible(key)){
                    // send query to my content provider
                    ContentResolver mContentResolver = parentActivity.getContentResolver();
                    Cursor resultCursor = mContentResolver.query(SimpleDhtProvider.CPUri, null, key, null, null);
                    Log.v(TAG, "QUERY: Query sent to my Content Provider.");

                    // send query response message to source id
                    handledMessage.setType(Message.TYPE.QUERY_RESPONSE);
                    handledMessage.setSenderId(MY_PORT);
                    handledMessage.setMessageText(createQueryResponseMessage(resultCursor));
                    mSender.sendMessage(buildJsonMessage(handledMessage), handledMessage.getSourceId());
                    Log.v(TAG, "QUERY: Sent Query Response to: " + handledMessage.getSourceId());

                }
                else{
                    // forward message to succesor
                    handledMessage.setSenderId(MY_PORT);
                    mSender.sendMessage(buildJsonMessage(handledMessage), successor);
                    Log.v(TAG, "QUERY: Forwarded Query message to: " + successor);
                }
                break;

            case QUERY_RESPONSE:
                // update the response
                queryResponse = handledMessage.getMessageText();
                // notify the condition variable
                queryLock.lock();
                queryCondVar.signal();
                queryLock.unlock();
                break;

            case QUERY_ALL:

                if(sourceId == MY_PORT) {
                    // message has come back to me, all nodes queried
                    // update the response
                    queryResponse = handledMessage.getMessageText();
                    // notify the condition variable
                    queryLock.lock();
                    queryCondVar.signal();
                    queryLock.unlock();
                }
                else {
                    ContentResolver mContentResolver = parentActivity.getContentResolver();
                    Cursor resultCursor = mContentResolver.query(SimpleDhtProvider.CPUri, null, "@", null, null);
                    Log.v(TAG, "QUERY: Query sent to my Content Provider.");

                    // forward query along with my data to the successor
                    handledMessage.setType(Message.TYPE.QUERY_ALL);
                    handledMessage.setSenderId(MY_PORT);
                    handledMessage.setMessageText(appendQueryResponseToMessage
                            (resultCursor, handledMessage.getMessageText()));
                    mSender.sendMessage(buildJsonMessage(handledMessage), successor);
                    Log.v(TAG, "QUERY: Query all forwarded to : " + successor);
                }
                break;

            case DELETE:
                if(amIResponsible(key)){
                    // send delete request to my content provider
                    ContentResolver mContentResolver = parentActivity.getContentResolver();
                    int result = mContentResolver.delete(SimpleDhtProvider.CPUri, key, null);
                    Log.v(TAG, "DELETE: Delete request sent to my Content Provider.");

                    // send delete response message to source id
                    handledMessage.setType(Message.TYPE.DELETE_RESPONSE);
                    handledMessage.setSenderId(MY_PORT);
                    handledMessage.setMessageText(result + "");
                    mSender.sendMessage(buildJsonMessage(handledMessage), handledMessage.getSourceId());
                    Log.v(TAG, "DELETE: Sent Delete Response to: " + handledMessage.getSourceId());

                }
                else{
                    // forward message to succesor
                    handledMessage.setSenderId(MY_PORT);
                    mSender.sendMessage(buildJsonMessage(handledMessage), successor);
                    Log.v(TAG, "DELETE: Forwarded Delete message to: " + successor);
                }
                break;
            case DELETE_RESPONSE:
                // update the response
                deleteResponse = handledMessage.getMessageText();
                // notify the condition variable
                deleteLock.lock();
                deleteCondVar.signal();
                deleteLock.unlock();
                break;

            case DELETE_ALL:
                if(sourceId == MY_PORT) {
                    // message has come back to me, deleted in all node
                    deleteResponse = "Delete all initiated by "
                            + handledMessage.getSourceId() +" completed.";
                    // notify the condition variable
                    deleteLock.lock();
                    deleteCondVar.signal();
                    deleteLock.unlock();
                }
                else {
                    ContentResolver mContentResolver = parentActivity.getContentResolver();
                    int result = mContentResolver.delete(SimpleDhtProvider.CPUri, "@", null);
                    Log.v(TAG, "DELETE: Delete request sent to my Content Provider.");

                    // forward delete request to the successor
                    handledMessage.setType(Message.TYPE.DELETE_ALL);
                    handledMessage.setSenderId(MY_PORT);

                    // update the number of rows deleted
                    result += Integer.parseInt(handledMessage.getMessageText());

                    handledMessage.setMessageText(result + "");
                    mSender.sendMessage(buildJsonMessage(handledMessage), successor);
                    Log.v(TAG, "DELETE: Delete all forwarded to : " + successor);
                }
                break;
        }
    }

    String appendQueryResponseToMessage(Cursor c, String message) {

        c.moveToFirst();

        JSONArray messageJSON = null;
        try {
            messageJSON = new JSONArray(message);
            while(!c.isAfterLast())
            {
                String key = c.getString(c.getColumnIndex("key"));
                String value = c.getString(c.getColumnIndex("value"));
                JSONObject jObject = new JSONObject();
                jObject.put("key", key);
                jObject.put("value", value);
                messageJSON.put(jObject);
                c.moveToNext();
            }
        } catch (JSONException e) {
            Log.e(TAG, "Exception: Improper Message Format.", e);
        }

        c.moveToFirst();
        return messageJSON.toString();
    }

    String createQueryResponseMessage(Cursor c){

        c.moveToFirst();

        StringWriter sWriter = new StringWriter();
        JsonWriter jWriter = new JsonWriter(sWriter);
        String jsonMessage = "";
        try {
            jWriter.beginArray();
            while(!c.isAfterLast())
            {
                String key = c.getString(c.getColumnIndex("key"));
                String value = c.getString(c.getColumnIndex("value"));
                jWriter.beginObject();
                jWriter.name("key").value(key);
                jWriter.name("value").value(value);
                jWriter.endObject();
                c.moveToNext();
            }
            jWriter.endArray();
            jWriter.close();
            jsonMessage = sWriter.toString();

            c.moveToFirst();

        } catch (IOException e) {
            Log.e(TAG, "Exception: IO Exception in JsonWriter.", e);
        }
        return jsonMessage;
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
        Log.v("LOOKUP", "Key: " +key +"\nKey Hash: " +keyHash +"\nPredecessor Hash: " +predecessorHash
                +"\nMy Hash: " +MY_HASH +"\nSuccessor Hash: " +successorHash);
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

    String createJoinInfoMessage()
    {
        StringWriter sWriter = new StringWriter();
        JsonWriter jWriter = new JsonWriter(sWriter);
        String joinResponse = "";
        try {
            jWriter.beginObject();
            jWriter.name("successor").value(MY_PORT);

            jWriter.endObject();
            jWriter.close();
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
            jWriter.close();
            joinResponse = sWriter.toString();

        } catch (IOException e) {
            Log.e(TAG, "Exception: IO Exception in JsonWriter.", e);
        }
        return joinResponse;

    }


}
