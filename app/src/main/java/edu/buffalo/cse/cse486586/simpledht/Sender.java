package edu.buffalo.cse.cse486586.simpledht;

import android.app.Activity;
import android.util.Log;

import java.io.DataOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;

public class Sender {

    static final String TAG = SenderRunnable.class.getSimpleName();
    Activity parentActivity;

    public void sendMessage(String message, int port)
    {
        // create a sender thread to send the message
        new Thread(new SenderRunnable(message, port)).start();
    }

    private class SenderRunnable implements Runnable {

        final String TAG = SenderRunnable.class.getSimpleName();
        String msgToSend;
        int port;

        SenderRunnable(String message, int port){
            this.msgToSend = message;
            this.port = port;
        }

        /**
         * Starts executing the active part of the class' code. This method is
         * called when a thread is started that has been created with a class which
         * implements {@code Runnable}.
         */
        @Override
        public void run() {
            Socket socket = null;
            try
            {
                socket = new Socket();
                socket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        port));
                if(!socket.isConnected())
                {
                    Log.v(TAG, "The socket to be used to send is closed");
                }
                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                out.writeUTF(msgToSend);
                out.close();
                socket.close();
            } catch(Exception e)
            {
                Log.e(TAG, "SenderRunnable Exception", e);
            }
        }
    }

}
