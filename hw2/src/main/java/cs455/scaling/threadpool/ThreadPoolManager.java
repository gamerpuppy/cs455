package cs455.scaling.threadpool;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

public class ThreadPoolManager {

    private static ThreadPoolManager theInstance = null;

    private final int poolSize;
    protected final int batchSize;
    protected final long batchTimeNanos;

    private final Queue<WorkerThread> workerQueue = new ArrayDeque<>();
    private final ArrayDeque<Batch> batchList = new ArrayDeque<>();

    public static ThreadPoolManager getInstance() {
        return theInstance;
    }

    public ThreadPoolManager(int poolSize, int batchSize, double batchTime) {
        this.poolSize = poolSize;
        this.batchSize = batchSize;
        this.batchTimeNanos = (long)(batchTime * 1000000000);

        theInstance = this;
    }

    public void createAndStartWorkers() {
        for(int i = 0; i < poolSize; i++){
            WorkerThread worker = new WorkerThread(this);
            Thread thread = new Thread(worker);
            thread.start();
        }
    }

    public void addTask(Task task)
    {
        synchronized(this)
        {
            if(this.workerQueue.isEmpty()) {
                addTaskWorkerQueueEmpty(task);

            } else {
                WorkerThread worker = this.workerQueue.peek();
                synchronized (worker) {
                    if (worker.getBatch() == null) {
                        worker.setBatch(new Batch(task, System.nanoTime()));
                        worker.notify();

                    } else {
                        List<Task> tasks = worker.getBatch().tasks;
                        tasks.add(task);
                        if (tasks.size() == this.batchSize) {
                            workerQueue.poll();
                            worker.notify();
                        }

                    }
                }

            }
        }
    }

    private void addTaskWorkerQueueEmpty(Task task) {
        boolean completed = false;
        for(Batch batch : this.batchList) {
            if(batch.tasks.size() < this.batchSize) {
                batch.tasks.add(task);
                completed = true;
            }
        }

        if(!completed) {
            this.batchList.add(new Batch(task, System.nanoTime()));
        }
    }

    protected Batch makeAvailable(WorkerThread worker)
    {
        synchronized (this)
        {
            if(this.workerQueue.isEmpty()) {
                if(batchList.isEmpty()) {
                    workerQueue.add(worker);
                    return null;

                } else {
                    Batch batch = batchList.poll();
                    if(batch.tasks.size() == this.batchSize) {
                        return batch;

                    } else {
                        workerQueue.add(worker);
                        return batch;

                    }

                }

            } else {
                workerQueue.add(worker);
                return null;

            }
        }
    }

    protected void removeFromQueue(WorkerThread worker) {
        synchronized (this) {
            workerQueue.remove(worker);
        }
    }

}




