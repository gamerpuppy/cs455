package cs455.scaling.server;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Batch {

    public final List<Task> tasks;
    public final long createTime;

    public Batch(Task task, long createTime) {
        this.tasks = new ArrayList<>();
        this.tasks.add(task);
        this.createTime = createTime;
    }

}
