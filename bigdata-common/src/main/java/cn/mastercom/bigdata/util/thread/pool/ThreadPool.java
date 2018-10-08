package cn.mastercom.bigdata.util.thread.pool;

import java.io.Serializable;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ThreadPool implements Serializable {

    private static transient ThreadPoolExecutor threadPoolExecutor;

    private static transient volatile AtomicInteger taskCount = new AtomicInteger(0);

    public static ThreadPoolExecutor getInstance() {
        if (null == threadPoolExecutor || threadPoolExecutor.isShutdown()) {
            synchronized (ThreadPool.class) {
                if (null == threadPoolExecutor || threadPoolExecutor.isShutdown()) {
                    threadPoolExecutor = new ThreadPoolExecutor(1, 5, 10, TimeUnit.SECONDS, new LinkedBlockingDeque<Runnable>());
                }
            }
        }
        return threadPoolExecutor;
    }

    public static void execute(Runnable runnable) {
        taskCount.getAndIncrement();
        getInstance().execute(runnable);
    }

    public static void waitForScheduled() {
        while (threadPoolExecutor.getCompletedTaskCount() < taskCount.get()) {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException ignored) {}
        }
    }

    public static void shutdown() {
        threadPoolExecutor.shutdown();
        threadPoolExecutor = null;
        taskCount.set(0);
    }
}
