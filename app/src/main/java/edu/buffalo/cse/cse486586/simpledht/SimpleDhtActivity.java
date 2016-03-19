package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentResolver;
import android.content.ContentValues;
import android.os.Bundle;
import android.app.Activity;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

public class SimpleDhtActivity extends Activity {

    static final String TAG = SimpleDhtActivity.class.getSimpleName();
    static Thread backgroundThread;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_simple_dht_main);
        
        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        findViewById(R.id.button3).setOnClickListener(
                new OnTestClickListener(tv, getContentResolver()));

        // setup to read the editText view and send when the send button is clicked
        final EditText keyEditText = (EditText) findViewById(R.id.keyEditText);
        final EditText valueEditText = (EditText) findViewById(R.id.valueEditText);

        final Button insertButton = (Button) findViewById(R.id.insertButton);
        insertButton.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {

                String key = keyEditText.getText().toString();
                String value = valueEditText.getText().toString();

                keyEditText.setText("");
                valueEditText.setText("");

                // insert message in my content provider
                ContentResolver mContentResolver = getContentResolver();
                ContentValues cv = new ContentValues();
                cv.put("key", key);
                cv.put("value", value);
                mContentResolver.insert(SimpleDhtProvider.CPUri, cv);
                Log.v(TAG, "Trying to insert the message.");
            }
        });


        // start everything in a background thread
        if(backgroundThread == null) {
            backgroundThread = new Thread(new Runnable() {
                public void run()
                {
                    Coordinator.getInstance(SimpleDhtActivity.this).start();
                }});
            backgroundThread.start();
        }

    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_simple_dht_main, menu);
        return true;
    }

}
