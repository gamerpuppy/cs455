package cs455.overlay.util;

public class StatisticsCollectorAndDisplay {

    private static int receiveTracker = 0;
    private static int sendTracker = 0;
    private static int messagesRelayed = 0;
    private static int sentSum = 0;
    private static int receivedSum = 0;

    public synchronized void incReceiveTracker(){
        receiveTracker++;
    }

    public synchronized void incSendTracker(){
        sendTracker++;
    }

    public synchronized void incMessagesRelayed(){
        messagesRelayed++;
    }

    public synchronized void incSentSum(int value){
        sentSum += value;
    }

    public synchronized void incReceivedSum(int value){
        receivedSum += value;
    }

    public static int getReceiveTracker() {
        return receiveTracker;
    }

    public static int getSendTracker() {
        return sendTracker;
    }

    public static int getMessagesRelayed() {
        return messagesRelayed;
    }

    public static int getSentSum() {
        return sentSum;
    }

    public static int getReceivedSum() {
        return receivedSum;
    }

}
