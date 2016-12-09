package com.canva.queue.service.file;

import com.canva.queue.common.exception.QueueServiceException;
import com.canva.queue.common.service.AbstractQueueService;
import com.canva.queue.message.ImmutableMessageQueue;
import com.canva.queue.message.MessageQueue;
import com.canva.queue.service.QueueService;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.DateTime;

import java.io.*;
import java.nio.file.Files;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.canva.queue.common.file.FileUtils.createDirectory;
import static com.canva.queue.common.file.FileUtils.createFile;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;

/**
 * Queue Service class providing standard operations to push, pull and remove messages from a file-based FIFO Queue.
 *
 * <p>This class creates a set of files and directories under {@code /home/.canva} by default (cf. {@link #DEFAULT_CANVA_DIR})
 * which can be overridden using {@link FileQueueService#FileQueueService(String, Integer, boolean)} constructor.
 * This service class works on a {@code queueName} basis whereby each queue is stored under its own directory
 * (e.g {@code /home/.canva/myqueue} containing messages file ({@code /home/.canva}/myqueue/messages) and lock file
 * ({@code /home/.canva}/myqueue/.lock).
 *
 * <p>Two level of locks are used in this class, one for threads and another for processes making it usable in a
 * multi-vm environment. The thread lock strategy is based on a list of locks per queue name stored in a concurrent
 * {@link #threadLockMap} map. As a result, this class service can be used concurrently by multiple threads that won't block
 * when working on different queues. However, when trying to access the same queue file, a pessimistic lock is applied
 * based on {@link ReentrantLock} locking strategy. In addition, a process lock is being used so that the queue files
 * can safely be accessed by several VMs. In order to make the queue files inter-process safe, a file lock per queue is created
 * and removed at the end of each operation (push, pull, delete). When a lock file is created and used by a process
 * the other processes working on the same queue, will run in a sleep loop (thread/process spinning approach) until
 * the lock is released (cf. file.mkdir() lock strategy).<br>
 *
 * <p>This class enables a daemon thread used for cleaning up any pending lock files left when exiting
 * abruptly the program.
 *
 * <p>Note that this thread can be turned off when invoking the constructor of this class through
 * {@link FileQueueService#FileQueueService(String, Integer, boolean)}.
 *
 * <p>The {@link FileQueueService.FileLockShutdownHook} can also but triggered separately using the following code snippet:<br>
 *
 * <pre>{@code
 *
 * // Add shutdown hook to release lock when application shutdown
 * FileQueueService.FileLockShutdownHook shutdownHook = new FileQueueService().new FileLockShutdownHook();
 * Runtime.getRuntime().addShutdownHook(new Thread(shutdownHook, "fileQueueService-shutdownHook"));
 * }</pre>
 *
 * @see QueueService
 * @see FileQueueService.FileLockShutdownHook
 */
public class FileQueueService extends AbstractQueueService {

    private static final Log LOG = LogFactory.getLog(FileQueueService.class);

    /**
     * Lock file name constant
     */
    private static final String LOCK_DIR_NAME = ".lock";
    /**
     * Messages file name constant
     */
    private static final String MESSAGES_FILE_NAME = "messages";
    /**
     * Temporary message file name constant used by {@link #push(String, Integer, String)}, {@link #pull(String)}
     * and {@link #delete(String, String)} methods.
     */
    private static final String NEW_MESSAGES_FILE_NAME = "messages.new";
    /**
     * Default base directory constant used for storing queue messages and related folders
     */
    private static final String DEFAULT_CANVA_DIR = System.getProperty("user.home") + File.separator + ".canva";

    /**
     * Concurrent list used for storing file lock instances per queue. This list is used by
     * {@link FileLockShutdownHook} thread class to remove any pending lock file (if left around)
     * before exiting the program.
     */
    private CopyOnWriteArrayList<File> lockFiles = Lists.newCopyOnWriteArrayList();
    /**
     * Concurrent map storing thread lock instance and related queue name pairs. It helps making this class thread-safe
     * by allowing multiple threads working on different queue to run concurrently without blocking.
     */
    private ConcurrentHashMap<String, ReentrantLock> threadLockMap = new ConcurrentHashMap<>();
    /**
     * Canva directory path
     */
    private String canvaDirPath;

    /**
     * Default constructor
     */
    protected FileQueueService() {
    }

    /**
     * Creates a {@link FileQueueService} instance given {@code path}, {@code timeoutInSecs} and {@code runShutdownHook}
     * arguments. The latter argument is used to run the shutdown hook that cleans up pending lock files.
     *
     * @param baseDirPath  Path used to initialize the canva base director
     * @param visibilityTimeoutInSecs  Visibility timeout in seconds
     * @param addShutdownHook  Boolean indicating if file lock shutdown hook should be added
     */
    public FileQueueService(String baseDirPath, Integer visibilityTimeoutInSecs, boolean addShutdownHook) {
        this.canvaDirPath = !StringUtils.isEmpty(baseDirPath) ? baseDirPath : DEFAULT_CANVA_DIR;
        this.visibilityTimeoutInSecs = defaultIfNull(visibilityTimeoutInSecs, MIN_VISIBILITY_TIMEOUT_SECS);

        // create canva base directory
        createDirectory(canvaDirPath);

        // add shutdown hook to remove lock file left when application shutdown
        addFileLockShutdownHook(addShutdownHook);
    }

    private void addFileLockShutdownHook(boolean addShutdownHook) {
        if(addShutdownHook) {
            Runtime.getRuntime().addShutdownHook(new Thread(new FileLockShutdownHook(), "fileQueueService-shutdownHook"));
        }
    }

    /**
     * Pushes a message at the end of a queue given {@code queueUrl}, {@code delaySeconds} and {@code messageBody} arguments.
     * If {@code delaySeconds} is provided, this method will set the visibility of the message pushed as per below:<br>
     *
     * <pre>{@code
     * visibility = currentTimeMillis + delayInMillis
     * }</pre>
     *
     * <p>This method uses a thread and process lock strategy to apply and support concurrency. The thread lock is
     * using a concurrent map that relates a {@link ReentrantLock} lock instance to a specific queue name. When invoking
     * this method, a thread and process lock are applied on a per-queue basis leaving other threads/processes free to
     * access any other queue without blocking.
     *
     * @param queueUrl  Queue url holding the queue name to extract
     * @param delaySeconds  Message visibility delay in seconds
     * @param messageBody  Message body to push
     * @throws QueueServiceException Thrown in case of issue while pushing the message to the queue
     */
    @Override
    public void push(String queueUrl, Integer delaySeconds, String messageBody) {
        String queue = fromUrl(queueUrl);
        File fileMessages = getMessagesFile(queue);
        File lock = getLockFile(queue);
        long visibleFrom = (delaySeconds != null) ? DateTime.now().getMillis() + TimeUnit.SECONDS.toMillis(delaySeconds) : 0L;

        lock(lock);

        try (PrintWriter pw = getPrintWriter(fileMessages)) {

                // create messageQueue
                MessageQueue messageQueue = MessageQueue.create(visibleFrom, messageBody);
                // add messageQueue to file
                pw.println(messageQueue.writeToString());

        } catch (IOException e) {
            throw new QueueServiceException("An error occurred while pushing messages [" + messageBody + "] to file '" + fileMessages.getPath() + "'", e);
        } finally {
            unlock(lock);
        }
    }

    protected PrintWriter getPrintWriter(File fileMessages) throws IOException {
        return new PrintWriter(new FileWriter(fileMessages, true));
    }

    /**
     * Pulls a visible message from the top of the queue given {@code queueUrl} argument, returns null otherwise.
     * The visibility of a message is determined according to its visibility timestamp (i.e equals to 0L or if
     * visibility < current millis). Any message with a visibility value above current time will be skipped
     * and considered invisible.
     *
     * <p>When the top message is pulled out from the queue, it is physically still kept in the file messages but with
     * a different visibility timestamp for a short period, or until the message is removed by calling {@link #delete(String, String)}.
     * During this period the message is considered invisible and cannot be accessed. When the invisibility period
     * has elapsed the message become available again and can be pulled from the queue.
     *
     * <p>When applying a change on the file queue, first this method creates a temporary new file messages with
     * the latest changes and then replaces the existing file messages with the new one.
     *
     * <p>This method uses a thread and process lock strategy to apply and support concurrency. The thread lock is
     * using a concurrent map that relates a {@link ReentrantLock} lock instance to a specific queue name. When invoking
     * this method, a thread and process locks are applied on a per-queue basis leaving other threads/processes free to
     * access any other queue without blocking.
     *
     * @param queueUrl  Queue url holding the queue name to extract
     * @return  MessageQueue instance made up with message body and receiptHandle identifier used to delete the message
     * @throws  QueueServiceException Thrown in case of issue while pulling the message from the queue
     * @throws  IllegalArgumentException If queue name cannot be extracted from queueUrl argument
     */
    @Override
    public MessageQueue pull(String queueUrl) {
        String queue = fromUrl(queueUrl);
        File fileMessages = getMessagesFile(queue);
        File newFileMessages = getNewMessagesFile(queue);
        File lock = getLockFile(queue);
        MessageQueue messageQueue = null;

        lock(lock);

        try {

            // read list of messages from file
            try (BufferedReader reader = getBufferedReader(fileMessages);
                 PrintWriter writer = getPrintWriter(newFileMessages)) {

                String[] linesArray = Iterators.toArray(reader.lines().iterator(), String.class);

                // create a reusable stream supplier
                Supplier<Stream<String>> streamSupplier =
                        () -> Stream.of(linesArray);

                // find first visible line to pull
                Optional<String> visibleLineToPull = streamSupplier
                        .get()
                        .filter(s -> isVisibleLine(s))
                        .findFirst();

                if (!visibleLineToPull.isPresent()) {
                    LOG.error("no visible messageQueue could be found in file '" + fileMessages.getPath() + "'");
                    return null;
                }

                // change pulled message visibility
                String updatedMessageLine = updateMessageVisibility(visibleLineToPull.get(), visibilityTimeoutInSecs);

                // write new visibility message to file
                writeNewVisibilityToFile(streamSupplier, writer, updatedMessageLine);

                // create messageQueue object
                messageQueue = MessageQueue.createFromLine(updatedMessageLine);
            }

            // replace file messages with new file
            replaceWithNewFile(fileMessages, newFileMessages);

        } catch (IOException e) {
            throw new QueueServiceException("An exception occurred while pulling from queue '" + queue + "'", e);
        } finally {
            unlock(lock);
        }

        return ImmutableMessageQueue.of(messageQueue);
    }

    protected BufferedReader getBufferedReader(File fileMessages) throws IOException {
        return Files.newBufferedReader(fileMessages.toPath());
    }

    /**
     * Deletes a message from the queue given {@code queueUrl} and {@code receiptHandle} arguments.
     *
     * <p>When applying a change on the file queue, first this method creates a temporary new file messages with
     * the latest changes and then replaces the existing file messages with the new one.
     *
     * <p>This method uses a thread and process lock strategy to apply and support concurrency. The thread lock is
     * using a concurrent map that relates a {@link ReentrantLock} lock instance to a specific queue name. When invoking
     * this method, a thread and process locks are applied on a per-queue basis leaving other threads/processes free to
     * access any other queue without blocking.
     *
     * @param queueUrl  Queue url holding the queue name to extract
     * @param receiptHandle  Receipt handle identifier
     * @throws QueueServiceException Thrown in case of issue while deleting the message from the queue
     */
    @Override
    public void delete(String queueUrl, String receiptHandle) {
        String queue = fromUrl(queueUrl);
        File fileMessages = getMessagesFile(queue);
        File newFileMessages = getNewMessagesFile(queue);
        File lock = getLockFile(queue);

        lock(lock);

        try {

            // read from messages file and retain all but the line containing the receipt handle
            List<String> linesWithoutReceiptHandle = getLinesFromFileMessages(fileMessages)
                    .stream()
                    .filter(s -> !s.contains(receiptHandle))
                    .collect(Collectors.toList());

            // write lines to temp file
            writeLinesToNewFile(newFileMessages, linesWithoutReceiptHandle);

            // replace file messages with new file
            replaceWithNewFile(fileMessages, newFileMessages);

        } catch (IOException e) {
            throw new QueueServiceException("An exception occurred while deleting receiptHandle '" + receiptHandle + "'", e);
        } finally {
            unlock(lock);
        }
    }

    protected void writeLinesToNewFile(File newFileMessages, List<String> linesWithoutReceiptHandle) throws IOException {
        Files.write(newFileMessages.toPath(), linesWithoutReceiptHandle);
    }

    protected List<String> getLinesFromFileMessages(File fileMessages) throws IOException {
        return Files.readAllLines(fileMessages.toPath());
    }

    /**
     * Writes the updated message line to new file messages. This method will be useful mainly for
     * the {@link #pull(String)} method after having identified the message line to pull.
     *
     * @param streamSupplier Stream supplier instance bound to the current file messages to read from
     * @param writer Writer instance bound to the new file messages
     * @param visibleLineToWrite Message line with updated visibility timestamp to write to file
     * @throws IllegalStateException If message id cannot be retrieved from message line
     */
    protected void writeNewVisibilityToFile(Supplier<Stream<String>> streamSupplier, PrintWriter writer, String visibleLineToWrite) {
        // retrieve message id
        final String messageId = retrieveMessageId(visibleLineToWrite)
                .orElseThrow(() -> new IllegalStateException("no message identifier found for record '" + visibleLineToWrite + "'"));

        streamSupplier
                .get()
                .forEach(s -> {

                    // update visible line with new timestamp
                    if (s.contains(messageId)) {
                        s = visibleLineToWrite;
                    }

                    // write to file
                    writer.println(s);
                });

    }

    /**
     * Update message visibility timeout according to {@code delaySeconds} argument and returns the new message line.
     *
     * @param messageLine Message line to update
     * @param delaySeconds  Message visibility delay in seconds
     * @return New Message line with updated visibility
     * @throws IllegalArgumentException If message is invalid as per {@link #validateMessage(String)} method
     */
    protected String updateMessageVisibility(String messageLine, Integer delaySeconds) {
        List<String> recordFields = Lists.newArrayList(Splitter.on(":").split(
                validateMessage(messageLine)));

        long visibility = DateTime.now().getMillis() + TimeUnit.SECONDS.toMillis(delaySeconds);

        return Joiner.on(":")
                .useForNull("")
                .join(recordFields.get(0), visibility, recordFields.get(2), recordFields.get(3), recordFields.get(4));
    }

    /**
     * Checks the visibility status of a message given {@code messageLine} argument.
     *
     * @param messageLine Message line to check visibility
     * @return Whether the message is visible
     * @throws IllegalArgumentException If message is invalid as per {@link #validateMessage(String)} method
     */
    protected boolean isVisibleLine(String messageLine) {

        try {
            if (!Strings.isNullOrEmpty(validateMessage(messageLine))) {
                return DateTime.now().getMillis() >
                        ObjectUtils.defaultIfNull(
                                Longs.tryParse(getMessageElement(messageLine, 1)),
                                0L);
            }
        } catch (NoSuchElementException nee) {
            LOG.error("An exception occurred while extracting visible status from message line '" + messageLine + "'", nee);
        }

        return false;
    }

    /**
     * Returns a message element given {@code messageLine} and its {@code position} arguments.
     *
     * @param messageLine Message line to check visibility
     * @param position Message position
     * @return Message element
     * @throws IllegalArgumentException If message is invalid as per {@link #validateMessage(String)} method
     */
    private String getMessageElement(String messageLine, int position) {
        return Iterables.get(Splitter.on(":").split(validateMessage(messageLine)), position);
    }

    /**
     * Validates a message given {@code messageLine} argument. This method essetially verifies the length of the message
     * passed in argument, that must be made of exactly 5 elements colon-separated as follows: "requeuecount:visibility:receipHandle:messageId:messageBody".
     *
     * @param messageLine Message line to check visibility
     * @return Message if valid
     * @throws IllegalArgumentException If message is invalid
     */
    protected String validateMessage(String messageLine) {
        // check validity of message
        if(Strings.isNullOrEmpty(messageLine)
                || Splitter.on(":").splitToList(messageLine).size() != 5) {
            throw new IllegalArgumentException("message line invalid '" + messageLine + "'");
        }

        return messageLine;
    }

    /**
     * Retrieves message id from a message line given {@code messageLine} argument.
     *
     * @param messageLine Message line to retrieve receipt handle
     * @return Optional receipt handle
     * @throws IllegalArgumentException If message is invalid
     */
    protected Optional<String> retrieveMessageId(String messageLine) {

        Optional<String> messageId = Optional.empty();

        if (!Strings.isNullOrEmpty(validateMessage(messageLine))) {
            messageId = Optional.ofNullable(getMessageElement(messageLine, 3));
        }

        return messageId;

    }

    /**
     * Replaces one file with another given {@code fileMessages} and {@code newFileMessages} arguments.
     *
     * @param fileMessages File messages to be replaced
     * @param newFileMessages New file messages to use
     * @throws IOException Exception thrown in case an issue occurred while replacing the files
     */
    protected void replaceWithNewFile(File fileMessages, File newFileMessages) throws IOException {
        requireNonNull(fileMessages, "Messages file must not be null");
        requireNonNull(newFileMessages, "Temp messages file must not be null");

        // delete initial messages file
        Files.deleteIfExists(fileMessages.toPath());

        // rename temp file
        newFileMessages.renameTo(new File(fileMessages.getPath()));

        // delete temporary file
        Files.deleteIfExists(new File(newFileMessages.getPath()).toPath());
    }

    /**
     * Returns a lock file instance given the {@code queueName} argument. This method creates the related queue directory
     * if it doesn't exist. The concrete creation of the lock file on disk is left to the {@link #lock(File)} method to enable
     * inter-process concurrency.
     *
     * @param queueName Queue name
     * @return File lock instance
     */
    protected File getLockFile(String queueName) {
        checkArgument(!Strings.isNullOrEmpty(queueName), "queueName must not be null");

        // create queue directory
        createDirectory(canvaDirPath + File.separator + queueName);

        return new File(canvaDirPath + File.separator + queueName + File.separator + LOCK_DIR_NAME);
    }

    /**
     * Returns the file messages given the {@cde queueName} argument. This method creates the related queue directory
     * if it doesn't exist along with the file messages itself on disk if non-existent.
     *
     * @param queueName Queue name
     * @return File messages instance
     */
    protected File getMessagesFile(String queueName) {
        checkArgument(!Strings.isNullOrEmpty(queueName), "queueName must not be null");

        // create queue directory
        createDirectory(canvaDirPath + File.separator + queueName);

        // create message file
        return createFile(new File(canvaDirPath + File.separator + queueName + File.separator + MESSAGES_FILE_NAME));
    }

    /**
     * Returns the new file messages given the {@cde queueName} argument. This method creates the related queue directory
     * if it doesn't exist along with the new file messages itself on disk if non-existent.
     *
     * @param queueName Queue name
     * @return File messages instance
     */
    protected File getNewMessagesFile(String queueName) {
        checkArgument(!Strings.isNullOrEmpty(queueName), "queueName must not be null");

        // create queue directory
        createDirectory(canvaDirPath + File.separator + queueName);

        // create new temporary message file
        return createFile(new File(canvaDirPath + File.separator + queueName + File.separator + NEW_MESSAGES_FILE_NAME));
    }

    /**
     * Triggers a thread lock based on a {@link ReentrantLock} instance retrieved from {@link #threadLockMap} map
     * along with an inter-process lock using the {@link File#mkdir()} method, given a {@code lock} file instance.
     *
     * <p>This method uses a pessimistic thread lock per queue that will provide exclusive access to a queue and
     * its related messages file to the first thread that acquires the lock. Any other threads trying to access
     * the same queue will block until the lock is released.
     *
     * <p>This method uses also a inter-process lock through the {@link File#mkdir()} atomic method, which makes the
     * program inter-process safe.
     *
     * <p>This method also adds the lock file created to a concurrent list {@link #lockFiles} so that it can
     * be used by the {@link FileLockShutdownHook} to remove any left lock file from disk if the program exits abruptly.
     *
     * @param lock File lock instance
     */
    protected void lock(File lock) {
        // thread lock
        getThreadLock(lock).lock();

        // process lock
        try {
            while (!lock.mkdir()) {
                Thread.sleep(50);
            }
        } catch (InterruptedException e) {
            throw new QueueServiceException("An exception occurred while creating lock file '" + lock + "'", e);
        }

        // keep track of lock file created for shutdown hook daemon thread
        lockFiles.add(lock);
    }

    /**
     * Releases thread and process locks enables by {@link #lock(File)} method. This method also remove the lock file
     * instance from the concurrent list {@link #lockFiles}.
     *
     * @param lock File lock instance
     */
    protected void unlock(File lock) {
        // process unlock
        lock.delete();

        // remove lock file from list
        lockFiles.remove(lock);

        // thread unlock
        getThreadLock(lock).unlock();

    }

    /**
     * Returns a {@link ReentrantLock} lock instance given the file {@code lock} argument.
     *
     * @param lock File lock instance
     * @return ReentrantLock instance related to the File lock argument
     */
    private ReentrantLock getThreadLock(File lock) {
        ReentrantLock threadLock = threadLockMap.get(lock.getPath());

        if (threadLock == null) {
            threadLock = new ReentrantLock();
            threadLockMap.put(lock.getPath(), threadLock);
        }

        return threadLock;
    }

    /**
     * Shutdown hook class that removes the remaining lock files before exiting the program. This class allows the
     * program to cleanly shutdown and prevents any issue while restarting it later on.
     *
     * @see FileQueueService#lockFiles
     */
    protected class FileLockShutdownHook implements Runnable {

        public void run() {
            if (!CollectionUtils.isEmpty(lockFiles)) {
                lockFiles.stream().map(File::delete);
                lockFiles.clear();
            }
        }
    }
}
