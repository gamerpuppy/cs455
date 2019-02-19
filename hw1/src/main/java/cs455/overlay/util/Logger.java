package cs455.overlay.util;

public class Logger {

    public static int logLevel = 1;

    public static void log(String s){
        if(logLevel > 0)
            System.out.println(s);
    }
}
