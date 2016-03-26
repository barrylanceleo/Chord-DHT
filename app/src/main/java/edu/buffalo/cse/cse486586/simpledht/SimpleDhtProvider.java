package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentProvider;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.SQLException;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.util.Log;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class SimpleDhtProvider extends ContentProvider {

    private static final String TAG = "SimpleDhtProvider";
    private static MainDatabaseHelper dbHelper;
    private static final String DB_NAME = "KeyValueDB";
    private static final String TABLE_NAME = "KeyValueTable";
    public static  Uri CPUri;

    @Override
    public boolean onCreate() {
        dbHelper = new MainDatabaseHelper(getContext());
        // initialize the content provider URI
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority("edu.buffalo.cse.cse486586.simpledht.provider");
        uriBuilder.scheme("content");
        CPUri =  uriBuilder.build();

        return true;
    }

    public boolean amIresponsible(String key)
    {
        Coordinator mCoordinator = Coordinator.getInstance();
        return mCoordinator.amIResponsible(key);
    }


    public void printCursor(Cursor c){

        Log.v("TAG", "Query Output:");
        c.moveToFirst();

        // print the rows
        while(!c.isAfterLast())
        {
            String returnKey = c.getString(c.getColumnIndex("key"));
            String returnValue = c.getString(c.getColumnIndex("value"));
            Log.v(TAG, "KEY: " + returnKey + " VALUE: " +returnValue);
            c.moveToNext();
        }
        c.moveToFirst();
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    public Uri plainInsert(Uri uri, ContentValues values)
    {

        SQLiteDatabase db = dbHelper.getWritableDatabase();
        // Performs the insert and returns the ID of the new row.
        long rowId = db.insertWithOnConflict(
                TABLE_NAME,            // The table to insert into.
                null,                // A hack, SQLite sets this column value to null
                // if values is empty.
                values,                 // A map of column names, and the values to insert
                // into the columns.
                SQLiteDatabase.CONFLICT_REPLACE
        );

        // If the insert succeeded, the row ID exists.
        if (rowId > 0) {
            // Creates a URI with the note ID pattern and the new row ID appended to it.
            Uri rowUri = ContentUris.withAppendedId(CPUri, rowId);

            // Notifies observers registered against this provider that the data changed.
            getContext().getContentResolver().notifyChange(rowUri, null);

            return rowUri;
        }
        else
        {
            // If the insert didn't succeed, then the rowID is <= 0. Throws an exception.
            throw new SQLException("Failed to insert row into " + uri);
        }
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {

        //display the message in the UI
        Coordinator mCoordinator = Coordinator.getInstance();
        String key = values.getAsString("key");
        String value = values.getAsString("value");
        Log.v(TAG, "INSERT: KEY: " + key + " VALUE: " + value);

        if(CPUri.toString().equals(uri.toString()))
        {
            // if this node is responsible for the key insert it here
            if(amIresponsible(key)){
                Uri outputUri = plainInsert(uri, values);
                Log.v(TAG, "INSERT: Inserting into CP." + "\nKEY: " + key + " VALUE: " + value);
                mCoordinator.displayMessage("Inserted: KEY: " +key +" VALUE: " +value);
                return outputUri;
            }
            else{
                // forward the insert message to the  successor
                Message insertMessage = mCoordinator.buildMessage(Message.TYPE.INSERT,
                        key, value);
                mCoordinator.mSender.sendMessage(mCoordinator.buildJsonMessage(insertMessage),
                        mCoordinator.successor);

                // wait till we receive a response from the inserted node
                mCoordinator.insertLock.lock();
                mCoordinator.insertCondVar.awaitUninterruptibly();
                mCoordinator.insertLock.unlock();
                Log.v(TAG, "INSERT: Response: " + mCoordinator.insertResponse);

                mCoordinator.insertResponse = "FAILED";

                return CPUri;
            }
        }
        else
        {
            throw new IllegalArgumentException("Unknown URI " + uri);
        }
    }

    Cursor parseQueryResponse(String queryResponse){

        JSONArray messageJSON = null;
        MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});
        try {
            messageJSON = new JSONArray(queryResponse);
            for(int i = 0; i < messageJSON.length(); i++){
                JSONObject jObject = messageJSON.getJSONObject(i);
                String key = jObject.getString("key");
                String value = jObject.getString("value");
                cursor.addRow(new String[]{key, value});
            }
        } catch (JSONException e) {
            Log.e(TAG, "Exception: Improper Message Format.", e);
        }

        cursor.moveToFirst();
        return cursor;
    }

    public Cursor plainQuery(Uri uri, String[] projection, String selection, String[] selectionArgs,
                             String sortOrder){
        SQLiteDatabase db = dbHelper.getReadableDatabase();
        SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
        qb.setTables(TABLE_NAME);

        Cursor c = qb.query(
                db,            // The database to query
                projection,    // The columns to return from the query
                selection,     // The columns for the where clause
                selectionArgs, // The values for the where clause
                null,          // don't group the rows
                null,          // don't filter by row groups
                sortOrder        // The sort order
        );

        c.moveToFirst();

        // Tells the Cursor what URI to watch, so it knows when its source data changes
        c.setNotificationUri(getContext().getContentResolver(), uri);
        return c;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {

        //display the message in the UI
        Coordinator mCoordinator = Coordinator.getInstance();
        Log.v(TAG, "QUERY: Request for KEY: " + selection);

        if(CPUri.toString().equals(uri.toString()))
        {

            if(selection.equals("@")){
                selection = null;
                selectionArgs = null;
                Cursor output = plainQuery(uri, projection, selection, selectionArgs, sortOrder);
                printCursor(output);
                return output;
            }

            if(selection.equals("*")){

                selection = null;
                selectionArgs = null;
                Cursor output = plainQuery(uri, projection, selection, selectionArgs, sortOrder);

                // forward query_all message to successor with my contents
                Message queryAllMessage = mCoordinator.buildMessage(Message.TYPE.QUERY_ALL, "", "");
                queryAllMessage.setMessageText(mCoordinator.createQueryResponseMessage(output));
                mCoordinator.mSender.sendMessage(mCoordinator.buildJsonMessage(queryAllMessage),
                        mCoordinator.successor);

                // wait till this message comes back tp us
                mCoordinator.queryLock.lock();
                mCoordinator.queryCondVar.awaitUninterruptibly();
                mCoordinator.queryLock.unlock();

                Log.v(TAG, "QUERY: Response: " + mCoordinator.queryResponse);
                output = parseQueryResponse(mCoordinator.queryResponse);
                printCursor(output);
                mCoordinator.queryResponse = "FAILED";

                return output;
            }

            if(amIresponsible(selection)){
                // given query format is not right, need to re-format
                selectionArgs = new String[]{selection};
                selection = "key=?";
                Cursor output = plainQuery(uri, projection, selection, selectionArgs, sortOrder);
                printCursor(output);
                return output;
            }
            else{
                // forward query to successor
                Message queryMessage = mCoordinator.buildMessage(Message.TYPE.QUERY,
                        selection, "");
                mCoordinator.mSender.sendMessage(mCoordinator.buildJsonMessage(queryMessage),
                        mCoordinator.successor);

                // wait till we receive a response from the responsible node
                mCoordinator.queryLock.lock();
                mCoordinator.queryCondVar.awaitUninterruptibly();
                mCoordinator.queryLock.unlock();

                Log.v(TAG, "QUERY: Response: " + mCoordinator.queryResponse);
                Cursor output = parseQueryResponse(mCoordinator.queryResponse);
                printCursor(output);
                mCoordinator.queryResponse = "FAILED";
                return output;
            }

        }
        else
        {
            throw new IllegalArgumentException("Unknown URI " + uri);
        }
    }

    public int plainDelete(Uri uri, String selection, String[] selectionArgs) {

        SQLiteDatabase db = dbHelper.getWritableDatabase();
        return db.delete(TABLE_NAME, selection, selectionArgs);

    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {

        //display the message in the UI
        Coordinator mCoordinator = Coordinator.getInstance();
        Log.v(TAG, "DELETE: Request for KEY: " + selection);

        if(CPUri.toString().equals(uri.toString()))
        {

            if(selection.equals("@")){
                selection = null;
                selectionArgs = null;
                int output = plainDelete(uri, selection, selectionArgs);
                return output;
            }

            if(selection.equals("*")){

                selection = null;
                selectionArgs = null;
                int output = plainDelete(uri, selection, selectionArgs);

                // forward delete_all message to successor
                Message deleteAllMessage = mCoordinator.buildMessage(Message.TYPE.DELETE_ALL, "", "");
                deleteAllMessage.setMessageText(output + "");
                mCoordinator.mSender.sendMessage(mCoordinator.buildJsonMessage(deleteAllMessage),
                        mCoordinator.successor);

                // wait till this message comes back tp us
                mCoordinator.deleteLock.lock();
                mCoordinator.deleteCondVar.awaitUninterruptibly();
                mCoordinator.deleteLock.unlock();

                Log.v(TAG, "DELETE: Response: " + mCoordinator.deleteResponse);
                output = Integer.parseInt(mCoordinator.deleteResponse);
                mCoordinator.deleteResponse = "FAILED";

                return output;
            }

            if(amIresponsible(selection)){
                // given query format is not right, need to re-format
                selectionArgs = new String[]{selection};
                selection = "key=?";
                int output = plainDelete(uri, selection, selectionArgs);
                return output;
            }
            else{
                // forward delete request to successor
                Message deleteMessage = mCoordinator.buildMessage(Message.TYPE.DELETE,
                        selection, "");
                mCoordinator.mSender.sendMessage(mCoordinator.buildJsonMessage(deleteMessage),
                        mCoordinator.successor);

                // wait till we receive a response from the responsible node
                mCoordinator.deleteLock.lock();
                mCoordinator.deleteCondVar.awaitUninterruptibly();
                mCoordinator.deleteLock.unlock();

                Log.v(TAG, "Delete: Response: " + mCoordinator.queryResponse);
                int output = Integer.parseInt(mCoordinator.deleteResponse);
                mCoordinator.queryResponse = "FAILED";
                return output;
            }

        }
        else
        {
            throw new IllegalArgumentException("Unknown URI " + uri);
        }
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    protected static final class MainDatabaseHelper extends SQLiteOpenHelper {

        /*
         * Instantiates an open helper for the provider's SQLite data repository
         * Do not do database creation and upgrade here.
         */
        MainDatabaseHelper(Context context) {
            super(context, DB_NAME, null, 1);
        }

        /*
         * Creates the data repository. This is called when the provider attempts to open the
         * repository and SQLite reports that it doesn't exist.
         */
        @Override
        public void onCreate(SQLiteDatabase db) {

            // Creates the main table
            db.execSQL("CREATE TABLE " + TABLE_NAME + " (_id INTEGER PRIMARY KEY AUTOINCREMENT, key TEXT UNIQUE, value TEXT);");
        }

        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion)
        {
            //do nothing
            return;
        }
    }
}
