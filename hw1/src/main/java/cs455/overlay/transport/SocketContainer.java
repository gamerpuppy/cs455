package cs455.overlay.transport;

import cs455.overlay.node.MessagingNode;
import cs455.overlay.node.Node;
import cs455.overlay.util.Logger;
import cs455.overlay.wireformats.Event;
import cs455.overlay.wireformats.EventFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;

public class SocketContainer {

    private Socket socket;
    private TCPReceiverThread receiver;
    private TCPSenderThread sender;
    private EventProcessor processor;

    public final String externalIpAddress;

    private Thread processorThread;
    private Thread receiverThread;
    private Thread senderThread;

    public static final int receiveBufSize = 32768*16;

    public SocketContainer(Socket socket) throws IOException{
        this.socket = socket;
        socket.setReceiveBufferSize(receiveBufSize);
        socket.setSendBufferSize(32768);

//        externalIpAddress = socket.getInetAddress().getHostAddress();

        String address = socket.getInetAddress().getHostName();
        int idx = address.indexOf('.');
        if(idx != -1)
            externalIpAddress = address.substring(0,idx);
        else
            externalIpAddress = address;

        Logger.log("starting socketcontainer on external ip:"+externalIpAddress);

        receiver = new TCPReceiverThread();
        sender = new TCPSenderThread();
        processor = new EventProcessor();

        processorThread = new Thread(processor);
        processorThread.start();

        receiverThread = new Thread(receiver);
        receiverThread.start();

        senderThread = new Thread(sender);
        senderThread.start();
    }

    class TCPReceiverThread implements Runnable {

        private DataInputStream din;

        public TCPReceiverThread() throws IOException {
            din = new DataInputStream(socket.getInputStream());
        }

        @Override
        public void run() {
            while(socket != null && !socket.isClosed()){
                try {
                    int dataLength = din.readInt();
                    byte[] data = new byte[dataLength];
                    din.readFully(data, 0, dataLength);
                    processor.addEvent(data);

                } catch (IOException e) {
                    Logger.log("receiver thread encountered an exception");
                    if(Node.theInstance.getClass() == MessagingNode.class)
                        System.exit(1);
//                    e.printStackTrace();
                    return;
                }
            }
        }

    }

    class EventProcessor implements Runnable {

        private LinkedList<byte[]> processQueue = new LinkedList<>();

        synchronized void addEvent(byte[] data){
            processQueue.add(data);
            this.notify();
        }

        synchronized byte[] getDataBlocking() throws InterruptedException {
            while (processQueue.isEmpty())
                this.wait();
            return processQueue.pollFirst();
        }

        @Override
        public void run() {
            while(true){
                try {
                    byte[] data = getDataBlocking();
                    Event event = EventFactory.createEvent(data);
                    Node.theInstance.onEvent(event, SocketContainer.this);

                } catch(Exception e){
                    Logger.log("event processor thread encountered an exception");
                    if(Node.theInstance.getClass() == MessagingNode.class)
                        System.exit(1);
//                    e.printStackTrace();
                    return;
                }
            }
        }

    }

    class TCPSenderThread implements Runnable{

        private boolean shutDown = false;
        private DataOutputStream dout;
        LinkedList<Data> sendQueue = new LinkedList<>();

        public TCPSenderThread() throws IOException {
            dout = new DataOutputStream(socket.getOutputStream());
        }

        public void addToQueue(Data data){
            synchronized (this) {
                sendQueue.add(data);
                this.notify();
            }
        }

        private void sendData(Data data) throws IOException {
            dout.writeInt(data.bytesToSend);
            dout.write(data.byteArray, 0 , data.bytesToSend);
            dout.flush();
        }

        @Override
        public void run() {
            while(true){
                try {
                    Data data;
                    synchronized (this) {
                        while (true) {
                            if (shutDown && sendQueue.isEmpty()) {
                                Logger.log("shutting down sender thread");
                                return;
                            } else if (sendQueue.isEmpty()) {
                                this.wait();
                            } else {
                                data = sendQueue.pollFirst();
                                break;
                            }
                        }

                        sendData(data);
                    }

                } catch(Exception e){

                    Logger.log("sender thread encountered an exception");
                    if(Node.theInstance.getClass() == MessagingNode.class)
                        System.exit(1);
//                    e.printStackTrace();
                    return;
                }
            }
        }

    }

    class Data {
        byte[] byteArray;
        int bytesToSend;

        Data(byte[] data, int bytesToSend){
            this.byteArray = data;
            this.bytesToSend = bytesToSend;
        }
    }

    // buf should be in writing state
    public void sendData(ByteBuffer buf){
        buf.flip();
        Data data = new Data(buf.array(), buf.limit());
        sender.addToQueue(data);
    }

    public void sendData(byte[] data){
        Data msg = new Data(data, data.length);
        sender.addToQueue(msg);
    }

    public boolean matchesIp(String ip){
        if(this.externalIpAddress.equals("localhost")){
            return Node.theInstance.myIpAddress.equals(ip);
        }

        return this.externalIpAddress.equals(ip);
    }

    public void shutDown(){
        processorThread.interrupt();
        receiverThread.interrupt();
        sender.shutDown = true;
        sender.notify();
    }

}
