package cs455.overlay.wireformats;

import java.nio.ByteBuffer;

public class EventFactory {

//    public static final int REGISTER_REQUEST = 0;
//    public static final int REGISTER_RESPONSE = 1;
//    public static final int DEREGISTRATION_REQUEST = 2;
//    public static final int DEREGISTRATION_RESPONSE = 3;
//    public static final int MESSAGING_NODES_LIST = 4;
//    public static final int LINK_WEIGHTS = 5;
//    public static final int TASK_INITIATE = 6;
//    public static final int TASK_COMPLETE = 7;
//    public static final int PULL_TRAFFIC_SUMMARY = 8;
//    public static final int TRAFFIC_SUMMARY = 9;
//    public static final int MESSAGE = 10;

    private static EventFactory theInstance = new EventFactory();

    public static EventFactory getTheInstance() {
        return theInstance;
    }

    public Event getEvent(byte[] data){
        if(data.length < 4){
            return null;
        }
        int type = ByteBuffer.wrap(data).getInt();
        switch(type){
            case 0: return new RegisterResponse();
        }

        return null;
    }


}
