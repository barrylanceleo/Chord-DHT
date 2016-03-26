package edu.buffalo.cse.cse486586.simpledht;

public class Message {

    public enum TYPE
    {
        JOIN_REQUEST(0),
        JOIN_REQUEST_FORWARD(1),
        JOIN_RESPONSE(2),
        JOIN_INFO(3),
        JOIN_COMPLETED(4),
        INSERT(5),
        INSERT_COMPLETED(6),
        QUERY(7),
        QUERY_ALL(8),
        QUERY_RESPONSE(9),
        DELETE(10),
        DELETE_ALL(11),
        DELETE_RESPONSE(12);

        private final int typeId;

        TYPE(int  typeId)
        {
            this.typeId = typeId;
        }

        int getVal()
        {
            return typeId;
        }

    }

    private TYPE type;
    private int sourceId;
    private int senderId;

    private String messageText;

    private String key;
    private String value;

    public String getMessageText() {
        return messageText;
    }

    public void setMessageText(String messageText) {
        this.messageText = messageText;
    }

    public int getSourceId() {
        return sourceId;
    }

    public void setSourceId(int sourceId) {
        this.sourceId = sourceId;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public int getSenderId() {
        return senderId;
    }

    /**
     * Returns a string containing a concise, human-readable description of this
     * object. Subclasses are encouraged to override this method and provide an
     * implementation that takes into account the object's type and data. The
     * default implementation is equivalent to the following expression:
     * <pre>
     *   getClass().getName() + '@' + Integer.toHexString(hashCode())</pre>
     * <p>See <a href="{@docRoot}reference/java/lang/Object.html#writing_toString">Writing a useful
     * {@code toString} method</a>
     * if you intend implementing your own {@code toString} method.
     *
     * @return a printable representation of this object.
     */
    @Override
    public String toString() {
        return ("Type: " + this.type + "\n" +
                "Source Id: " + this.sourceId + " Sender Id: " + this.senderId +
                "\nMessage Text: " + this.messageText);
    }

    public void setSenderId(int senderId) {
        this.senderId = senderId;
    }

    public TYPE getType() {
        return type;
    }

    public void setType(TYPE type) {
        this.type = type;
    }

    public int getTypeId() {
        return type.typeId;
    }

    public void setTypeWithId(int typeId) {
        for (TYPE type : TYPE.values()) {
            if (type.typeId == typeId) {
                this.type = type;
            }
        }
    }
}
