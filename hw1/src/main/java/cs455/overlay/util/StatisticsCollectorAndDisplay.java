package cs455.overlay.util;

public class StatisticsCollectorAndDisplay {


    private Integer receiveTracker = 0;
    private Integer sendTracker = 0;
    private Integer relayTracker = 0;
    private Long sentSum = 0L;
    private Long receivedSum = 0L;

    public synchronized void incReceiveTracker(){
        receiveTracker++;
    }

    public synchronized void incSendTracker(){
        sendTracker++;
    }

    public synchronized void incMessagesRelayed(){
        relayTracker++;
    }

    public synchronized void incSentSum(int value){
        sentSum += value;
    }

    public synchronized void incReceivedSum(int value){
        receivedSum += value;
    }

    public synchronized void reset(){
        receiveTracker = 0;
        sendTracker = 0;
        relayTracker = 0;
        sentSum = 0L;
        receivedSum = 0L;
    }

    public int getReceiveTracker() {
        return receiveTracker;
    }

    public int getSendTracker() {
        return sendTracker;
    }

    public int getRelayTracker() {
        return relayTracker;
    }

    public long getSentSum() {
        return sentSum;
    }

    public long getReceivedSum() {
        return receivedSum;
    }

}
