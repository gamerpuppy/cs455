package cs455.overlay.transport;

import cs455.overlay.node.Node;
import cs455.overlay.util.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

public class TCPServerThread implements Runnable {

    private static TCPServerThread theInstance = null;

    private Node node;
    public Selector selector;
    private ServerSocketChannel serverSocketChannel;
    private EventQueue eventQueue;

    public SocketChannel registry = null;
//    private Map<SocketChannel, >

    public TCPServerThread(Node node){
        this.node = node;
    }

    public void setupMessaging(InetSocketAddress regAddr, int portToBind) throws IOException{
        this.selector = Selector.open();
        this.serverSocketChannel = ServerSocketChannel.open();
        this.serverSocketChannel.configureBlocking(false);
        this.serverSocketChannel.register(this.selector, SelectionKey.OP_ACCEPT);
        this.bindServerSocket(serverSocketChannel.socket(), portToBind);

        this.registry = SocketChannel.open(regAddr);
        this.registry.configureBlocking(false);
        this.registry.register(this.selector, SelectionKey.OP_READ);

        this.node.ipAddress = InetAddress.getLocalHost().getCanonicalHostName();
        this.node.port = serverSocketChannel.socket().getLocalPort();

        Logger.log("bound to " + node.ipAddress+":"+node.port);
    }

    public void setupRegistry(int port) throws IOException {
        this.selector = Selector.open();
        this.serverSocketChannel = ServerSocketChannel.open();
        this.serverSocketChannel.configureBlocking(false);
        this.serverSocketChannel.register(this.selector, SelectionKey.OP_ACCEPT);
        this.serverSocketChannel.socket().bind(new InetSocketAddress(port));

        this.node.ipAddress = InetAddress.getLocalHost().getCanonicalHostName();
        this.node.port = serverSocketChannel.socket().getLocalPort();

        Logger.log("bound to " + node.ipAddress+":"+node.port);
    }

    @Override
    public void run() {
        try {

            this.eventQueue = new EventQueue(node);
            Thread eventQueueThread = new Thread(eventQueue);
            eventQueueThread.start();

            while(true){

                this.selector.select();
                Iterator<SelectionKey> keys = this.selector.selectedKeys().iterator();
                while(keys.hasNext()){
                    SelectionKey key = keys.next();
                    keys.remove();

                    if(!key.isValid()){
                        continue;
                    }

                    if(key.isAcceptable()){
                        this.register(key);
                    }

                    if(key.isReadable()){
                        this.read(key);
                    }
                }

            }

        } catch(Exception e){
            e.printStackTrace();
        }
    }

    private void register(SelectionKey key) throws IOException {
        SocketChannel socketChannel = ((ServerSocketChannel)key.channel()).accept();
        socketChannel.configureBlocking(false);
        socketChannel.register(this.selector, SelectionKey.OP_READ);
    }

    private void read(SelectionKey key) throws IOException {

        ByteBuffer buf = ByteBuffer.allocate(32768);
        SocketChannel socketChannel = (SocketChannel) key.channel();

        int bytesRead = socketChannel.read(buf);
        if(bytesRead == -1){
            socketChannel.close();
            System.out.println("connection closed");
        } else {
            buf.flip();
            try {
                eventQueue.queue.put(new EventQueue.Input(bytesRead, buf, socketChannel));
            } catch(InterruptedException e){}
        }
    }

    private void bindServerSocket(ServerSocket s, int port) throws IOException{
        while(!s.isBound() && port < 0xFFFF){
            try {
                s.bind(new InetSocketAddress("127.0.0.1", port));
            } catch(Exception e){
                Logger.log("exception trying to bind to port "+ port);
                port++;
            }
        }
        if(!s.isBound()){
            throw new IOException();
        }

        Logger.log("bound to "+s.getLocalPort());

    }

    public static TCPServerThread getTheInstance(){
        return TCPServerThread.theInstance;
    }

    public static void setTheInstance(TCPServerThread instance){
        TCPServerThread.theInstance = instance;
    }


}
