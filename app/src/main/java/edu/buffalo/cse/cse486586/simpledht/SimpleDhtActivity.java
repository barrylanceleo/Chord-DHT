package edu.buffalo.cse.cse486586.simpledht;

import android.os.Bundle;
import android.app.Activity;
import android.text.method.ScrollingMovementMethod;
import android.view.Menu;
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

        // start everything in a background thread
        if(backgroundThread == null) {
            backgroundThread = new Thread(new Runnable() {
                public void run()
                {
                    Coordinator.getInstance(SimpleDhtActivity.this);
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
