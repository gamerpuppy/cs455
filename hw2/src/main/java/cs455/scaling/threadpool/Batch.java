package cs455.scaling.threadpool;

import java.util.ArrayList;
import java.util.List;

class Batch {

    protected final List<Task> tasks;
    protected final long createTime;

    Batch(Task task, long createTime) {
        this.tasks = new ArrayList<>();
        this.tasks.add(task);
        this.createTime = createTime;
    }

}
