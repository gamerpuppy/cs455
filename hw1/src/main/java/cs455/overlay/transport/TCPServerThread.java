package cs455.overlay.transport;

import cs455.overlay.node.Node;
import cs455.overlay.util.Logger;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

public class TCPServerThread implements Runnable {

    public static final int defaultPort = 10000;

    public Selector selector;
    public ServerSocketChannel serverSocketChannel;

    public TCPServerThread(int portToBind) throws IOException {
        this.init();
        this.serverSocketChannel.socket().bind(new InetSocketAddress(portToBind));
        Logger.log("bound to "+Node.theInstance.myIpAddress+":"+serverSocketChannel.socket().getLocalPort());
    }

    public TCPServerThread() throws IOException {
        this.init();
        this.bindServerSocket(serverSocketChannel.socket(), defaultPort);
    }

    private void init() throws IOException{
        this.selector = Selector.open();
        this.serverSocketChannel = ServerSocketChannel.open();
        this.serverSocketChannel.configureBlocking(false);
        this.serverSocketChannel.register(this.selector, SelectionKey.OP_ACCEPT);
    }

    @Override
    public void run() {
        try {
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

    private void bindServerSocket(ServerSocket s, int port) throws IOException{
        while(!s.isBound() && port < 0xFFFF){
            try {
                s.bind(new InetSocketAddress("127.0.0.1", port));
            } catch(Exception e){
                Logger.log("exception trying to bind to myPort "+ port);
                port++;
            }
        }
        if(!s.isBound()){
            throw new IOException();
        }

        Logger.log("bound to "+s.getLocalPort());

    }

}
