package cs455.overlay.wireformats;

import java.nio.ByteBuffer;

public class EventFactory {

    public static final int REGISTER_REQUEST = 0;
    public static final int REGISTER_RESPONSE = 1;
    public static final int DEREGISTER_REQUEST = 2;
    public static final int DEREGISTER_RESPONSE = 3;
    public static final int MESSAGING_NODES_LIST = 4;
    public static final int LINK_WEIGHTS = 5;
    public static final int TASK_INITIATE = 6;
    public static final int TASK_COMPLETE = 7;
    public static final int TRAFFIC_SUMMARY_REQUEST = 8;
    public static final int TRAFFIC_SUMMARY_RESPONSE = 9;
    public static final int MESSAGE = 10;

    public static Event createEvent(int datalen, ByteBuffer buf){

        int type = buf.getInt();

        switch(type){
            case 0: return new RegisterRequest(buf);
            case 1: return new RegisterResponse(buf);
            case 2: return new DeregisterRequest(buf);
            case 3: return new DeregisterResponse(buf);
            case 4: return new MessagingNodesList(buf);
            case 5: return new LinkWeights(buf);
            case 6: return new TaskInitiate(buf);
            case 7: return new TaskComplete(buf);
            case 8: return new TrafficSummaryRequest(buf);
            case 9: return new TrafficSummaryResponse(buf);
            case 10: return new Message(buf);
            default: return new InvalidEvent();
        }

    }


}
