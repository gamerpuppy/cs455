package cs455.scaling.threadpool;

public class WorkerThread implements Runnable {

    private final ThreadPoolManager manager;
    private Batch batch = null;

    protected WorkerThread(ThreadPoolManager manager) {
        this.manager = manager;
    }

    protected void setBatch(Batch batch) {
        this.batch = batch;
    }

    protected Batch getBatch() {
        return this.batch;
    }

    private void waitForBatch() throws InterruptedException {
        synchronized (this) {
            this.batch = this.manager.makeAvailable(this);

            while (this.batch == null) {
                this.wait();
            }

            while(true) {
                long timeNow = System.nanoTime();
                long nanoTimeFinished = this.batch.createTime + this.manager.batchTimeNanos;
                long millisToWait = (nanoTimeFinished - timeNow) / 1000000;

                if(this.batch.tasks.size() == this.manager.batchSize)
                    break;

                if(millisToWait <= 0)
                    break;

                this.wait(millisToWait);
            }
        }
        manager.removeFromQueue(this);
    }

    @Override
    public void run() {
        try {

            while (true)
            {
                waitForBatch();

                for (Task task : batch.tasks)
                    task.execute();

                batch = null;
            }

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

}
