package cs455.overlay.util;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class BufUtils {

    public static String getString(ByteBuffer buf){
        byte[] data = buf.array();

        int i = buf.position();
        while(i < buf.limit()){
            if(data[i] == 0){
                break;
            }
            i++;
        }

        String ret = new String(data,
                buf.position(),
                i-buf.position(),
                StandardCharsets.US_ASCII);

        buf.position(i+1);
        return ret;
    }

    public static void putString(ByteBuffer buf, String s){
        buf.put(s.getBytes(StandardCharsets.US_ASCII));
        buf.put((byte)0);
    }

}
