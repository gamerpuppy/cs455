package cs455.overlay.util;

public class Logger {

    public static int logLevel = 1;

    public final static int LOG_LEVEL_ON = 1;
    public final static int LOGLEVEL_OFF = 0;

    public static void log(String s){
        if(logLevel > 0)
            System.out.println(s);
    }
}
