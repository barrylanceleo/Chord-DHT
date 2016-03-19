package edu.buffalo.cse.cse486586.simpledht;

import android.util.Log;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.concurrent.BlockingQueue;

/**
 * Created by barry on 3/18/16.
 */
public class Listener {

    private static final String TAG = Listener.class.getSimpleName();
    private static final int LISTENING_PORT = 10000;
    private BlockingQueue<String> sharedMessageQueue;


    Listener(BlockingQueue sharedMessageQueue)
    {
        this.sharedMessageQueue = sharedMessageQueue;
        new Thread(new ListenerRunnable()).start();
    }

    private class ListenerRunnable implements Runnable {

        final String TAG = ListenerRunnable.class.getSimpleName();

        /**
         * Starts executing the active part of the class' code. This method is
         * called when a thread is started that has been created with a class which
         * implements {@code Runnable}.
         */
        @Override
        public void run() {

            ServerSocket listenerSocket = null;
            try {
                listenerSocket = new ServerSocket(LISTENING_PORT);
            } catch (IOException e) {
                Log.e(TAG, "Can't create a ServerSocket", e);
            }

            //noinspection InfiniteLoopStatement
            while (true) {
                try {
                    Socket server = listenerSocket.accept();
                    DataInputStream in = new DataInputStream(server.getInputStream());
                    String mesRecvd = in.readUTF();
                    server.close();
                    // add the message to the coordinator's queue
                    try {
                        sharedMessageQueue.put(mesRecvd);
                    } catch (InterruptedException e) {
                        Log.e(TAG, "Unable to add message to the coordinator's queue.", e);
                    }
                } catch (SocketTimeoutException s) {
                    Log.e(TAG, "Receiver timed out!");
                } catch (IOException e) {
                    Log.e(TAG, Log.getStackTraceString(e));
                }
            }
        }
    }

}
