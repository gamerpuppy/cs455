package cs455.scaling.server.tasks;

import cs455.scaling.threadpool.Task;
import cs455.scaling.threadpool.ThreadPoolManager;
import cs455.scaling.util.SHA;

import java.nio.charset.StandardCharsets;

public class HashTask implements Task {

    private final SocketChannelWrapper wrapper;
    private final byte[] bytes;

    public HashTask(SocketChannelWrapper wrapper, byte[] bytes) {
        this.wrapper = wrapper;
        this.bytes = bytes;
    }

    @Override
    public void execute() {
        String hash = SHA.SHA1FromBytesPadded(bytes, 40);
        WriteTask task = new WriteTask(wrapper, hash.getBytes(StandardCharsets.US_ASCII));
        ThreadPoolManager.getInstance().addTask(task);
    }

}
