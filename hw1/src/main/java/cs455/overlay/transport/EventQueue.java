package cs455.overlay.transport;

import cs455.overlay.node.Node;
import cs455.overlay.wireformats.Event;
import cs455.overlay.wireformats.EventFactory;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class EventQueue implements Runnable {

    public BlockingQueue<Input> queue = new ArrayBlockingQueue<>(25000);


    private final Node node;

    public EventQueue(Node node){
        this.node = node;
    }

    @Override
    public void run() {

        while(true){
            try {
                Input input = queue.poll(10, TimeUnit.MILLISECONDS);

                if(input != null) {

                    Event event = EventFactory.createEvent(input.bytesRead, input.buffer);
                    this.node.onEvent(event, input.channel);
                }
            } catch(InterruptedException e){
                e.printStackTrace();
            }
        }

    }

    public static class Input {

        final int bytesRead;
        final ByteBuffer buffer;
        final SocketChannel channel;

        public Input(int bytesRead, ByteBuffer buffer, SocketChannel channel){
            this.bytesRead = bytesRead;
            this.buffer = buffer;
            this.channel = channel;
        }

    }

}
