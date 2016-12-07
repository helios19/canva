package com.canva.queue.common.service;

import com.canva.queue.service.QueueService;
import com.canva.queue.service.memory.InMemoryQueueService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.annotation.PreDestroy;

import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;

/**
 * Queue Service abstract class declaring common constants and methods used across service implementation classes.
 *
 * @see QueueService
 */
public abstract class AbstractQueueService implements QueueService {

    /**
     * Minimum visibility timeout in second
     */
    protected static final int MIN_VISIBILITY_TIMEOUT_SECS = 60;

    /**
     * Visibility timeout initialized with the default constant {@code MIN_VISIBILITY_TIMEOUT_SECS}.
     */
    protected int visibilityTimeoutInSecs = MIN_VISIBILITY_TIMEOUT_SECS;

    /**
     * Flag to control the execution of the visibility collector
     */
    protected boolean stopVisibilityCollector = false;


    /**
     * Closes resources created by the {@link InMemoryQueueService} instance as well as stopping the execution of the
     * {@link InMemoryQueueService.VisibilityMessageMonitor}.
     *
     * <p>If used inside a container the call of this method will be done automatically when the instance of this class
     * is about to be garbage collected, as per {@link PreDestroy} annotation logic. However, in a standalone environment
     * this method will need to be called manually possibly in a finally block.
     */
    @PreDestroy
    public void close() {
        stopVisibilityCollector = true;
    }

    /**
     * Visibility message monitor resetting the visibility of queue messages if expired. As per the implementation of
     * the {@link QueueService#push(String, Integer, String)} and {@link QueueService#pull(String)} methods,
     * the visibility of a message can be altered when it is pushed or pulled from the queue. A timestamp is used to determine
     * the inaccessibility period of a message, which after elapsed, must be reset to {@code 0L}.
     *
     * <p>The {@link AbstractVisibilityMonitor} implementation classes are thread-safe watchers that can run concurrently
     * with other threads operating on the {@link QueueService} public methods (push, pull, delete). They run on a per
     * queueName basis, locking each queue using a thread lock and a process lock file. When running on a given queue,
     * all operations executed by other threads are suspended until both locks are released. Note that, any operations
     * carried out on other queues are meanwhile un-impacted and can be run concurrently.
     */
    protected abstract class AbstractVisibilityMonitor implements Runnable {

        private final Log LOG = LogFactory.getLog(AbstractVisibilityMonitor.class);

        protected static final long VISIBILITY_MONITOR_PAUSE_TIMER = 2000L;

        /**
         * Visibility pause timer in second
         */
        protected long pauseTimer = VISIBILITY_MONITOR_PAUSE_TIMER;

        public AbstractVisibilityMonitor() {
        }

        public AbstractVisibilityMonitor(long pauseTimer) {
            this.pauseTimer = defaultIfNull(pauseTimer, VISIBILITY_MONITOR_PAUSE_TIMER);
        }

        @Override
        public void run() {

            LOG.info("Starting VisibilityMessageMonitor...");

            while (!stopVisibilityCollector) {

                checkMessageVisibility();

                // pause after each checker cycle
                try {
                    Thread.sleep(pauseTimer);
                } catch (InterruptedException e) {
                    LOG.error("An exception occurred while pausing the visibility collector", e);
                }

            }

        }

        /**
         * Checks and resets queue messages visibility.
         */
        protected abstract void checkMessageVisibility();
    }

}
