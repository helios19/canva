This project is a message-queue API implementation replicating the functionality of SQS Queue Service. It provides three
Queue Service implementation classes:

- InMemoryQueueService: In-memory queue service
- FileQueueService: File-based queue service
- SqsQueueService: Adapter to connect to AWS SQS Queue


How to use it
--

To use this message-queue API, first build the package jar library by running the following maven command:

```
mvn clean package
```

Then, import the jar into your environment or simply place it into your classpath, and add the following snippet 
code into your program:

*In-memory Queue Service*
```
    Integer visibilityTimeoutInSecs = 2;
    boolean runVisibilityCollector = true;

    InMemoryQueueService queueService = new InMemoryQueueService(visibilityTimeoutInSecs, runVisibilityCollector);
        
    // push
    String queueUrl = "http://sqs.us-east-2.amazonaws.com/123456789012/MyQueue";
    Integer delaySeconds =  null;
    String messageBody = "my message body";
    
    queueService.push(queueUrl, delaySeconds, messageBody);


    // pull
    MessageQueue messageQueue = queueService.pull(queueUrl);


    // delete
    String receiptHandle = messageQueue.getReceiptHandle();

    queueService.delete(queueUrl, receiptHandle);

```

*File-based Queue Service*
```
    String baseDirPath = null;
    Integer visibilityTimeoutInSecs = 2;
    boolean runVisibilityCollector = true;
    boolean addShutdownHook = true;

    // file queue service
    FileQueueService queueService = new FileQueueService(baseDirPath, visibilityTimeoutInSecs, runVisibilityCollector, addShutdownHook);
    
    // push
    String queueUrl = "http://sqs.us-east-2.amazonaws.com/123456789012/MyQueue";
    Integer delaySeconds =  null;
    String messageBody = "my message body";
    
    queueService.push(queueUrl, delaySeconds, messageBody);


    // pull
    MessageQueue messageQueue = queueService.pull(queueUrl);


    // delete
    String receiptHandle = messageQueue.getReceiptHandle();

    queueService.delete(queueUrl, receiptHandle);

```

Please refer to the respective javadoc of the Queue Service implementation class for a comprehensive description of the 
constructor argument options.



Design and architecture decisions
--

The solution provided has been designed in order primarily to be simple enough to be used in a local and/or testing
environment, yet be easily maintainable and still performant in a multi-threads and inter-processes context.

The solution implemented tries to follows some of the most important characteristics of AWS SQS Queue Service, as per
the online documentation:

- multiplicity
A queue supports many producers and many consumers.

- delivery
A queue strives to deliver each message exactly once to exactly one consumer,
but guarantees at-least once delivery (it can re-deliver a message to a
consumer, or deliver a message to multiple consumers, in rare cases).

- order
A queue strives to deliver messages in FIFO order, but makes no guarantee
about delivery order.

- reliability
When a consumer receives a message, it is not removed from the queue.
Instead, it is temporarily suppressed (becomes "invisible").  If the consumer
that received the message does not subsequently delete it within within a
timeout period (the "visibility timeout"), the message automatically becomes
visible at the head of the queue again, ready to be delivered to another
consumer.    


Hereafter is a shortlist of the other technical aspects considered while implementing the system:


#### Design Principles and best practices

The message-queue API has been implemented with the following best practices and design principles in mind:

- BDD: Each Queue Service public method has been implemented along with its related tests covering the expected
functional behaviors
- SOLID principles: Single Responsibility, Opened/Closed, Liskov substitution, Interface segregation, Dependency inversion
- 10-50-600: Not more than 10 classes per package, methods are not exceeding 50 lines and classes length are 
below 600 lines
- Comments and documentation on all public methods and classes


#### Concurrency In Multi-Thread and Inter-Process Environment

In order to achieve concurrency in a multi-thread and inter-process environment the following patterns have been applied:


###### Atomic operation

The JDK platform provides some classes supporting atomic operations through the CAS feature provided by most of the OS systems.
This type of concurrency mechanisms is preferable over the use of Locks mainly for performance reasons.

The implemented message-queue API takes advantage of this type of classes to offer thread-safety while running multiple
consumers and producers concurrently. In particular, the In-memory message-queue service uses the ```AtomicLong.compareAndSet```
method to atomically update the visibility timestamp of a message while being accessed by multiple threads.

###### Concurrent data structure

Again here, the JDK gathers a raft of concurrent structures aiming at facilitating the development of thread-safe program
by implementing some of the most efficient paradigms and mechanisms such as lock-free, non-blocking, optimistic-locking
or standard pessimistic-locking.

The system implemented uses some of these data structures such as:
            
- ConcurrentLinkedQueue: one of the preferred non-blocking structure when it comes to implement a multi-threads consumers
producers API. The In-memory Queue Service implementation uses it to store the messages.
    
- ConcurrentHashMap: Provides bucket lock mechanism rather than blocking on the map instance. The File-based
Queue Service makes use of it to store a list of Reentrant lock per queue name.

- CopyOnWriteArrayList: Another concurrent list implementation providing bucket locking. The File-based Queue
Service uses it to store the list of queue name accessed by the Service instance at runtime in order for the
visibility watcher thread to limit the number of queue folder to look into during its visibility cycle. The
File-based Queue Service uses also this structure to save the list of lock file created (and not yet deleted)
by the system to let the FileLockShutdownHook thread remove them if still around when the program exit abruptly. 


###### Immutability

Immutability is probably one of the best approach when dealing with concurrency and greatly helps any system to scale out.
The message-queue API implemented uses immutability mainly to protect its internal state of the messages from any external
intervention (e.g from the API callers). Indeed, when pulling a message from the queue, the system returns an immutable
version of the message (```ImmutableMessageQueue``` class) preventing any external changes on the real message stored 
in the queue.

Note that the ```MessageQueue``` class itself is not immutable (currently only the AtomicLong visibility property can change)
as it provides some useful methods to update the visibility timestamp in a atomic manner.


###### Inter-process locking using mkdir

As per the well described blog post (https://engineering.canva.com/2015/03/25/hermeticity/), in order to achieve inter-process 
safety, the system uses a .lock folder created using mkdir and leverages on the atomicity of this command on POSIX filesystem.
All message operations provided the File-based Queue Service class are synchronized using a .lock folder created with mkdir.

In addition to the .lock folder, and in order to properly achieve thread-safety while dealing with pull/push/delete methods, 
a map of ```ReentrantLock``` instance per queue name is being used to exclusively let a thread access to a particular
queue file. This approach of lock instance per queue provides a good compromise between performance and state consistency,
as the thread blocking lock is applied per queue leaving other threads free to access concurrently any other queue without blocking.



#### Visibility Timeout Feature

In order to implement the visibility timeout feature as per the AWS SQS Queue Service, a watcher thread has been implemented
that runs periodically in the background and check the visibility timestamp of each message on a per queue basis.

This external visibility watcher approach offers the advantage of decoupling the visibility timeout feature from the rest
of the Queue Service operations (push/pull/delete) and helps to better maintain the system.

Currently, a watcher is created for each instance of Queue Service, and will focus on the visibility timeout of the messages
stored in the queue name passed in to the Queue Service instance methods. Though, a caller still has the possibility to
turn off this watcher by either calling the ```close()``` or by setting the ```runVisibilityWatcher``` property to false
when invoking the Queue Service constructor.





Trade-offs and limitations
--

The watcher approach for implementing the visibility timeout is a simple yet effective solution to address the issue
of maintaining the visibility property state per message queue. Other runtime platforms have come up with similar
solutions, such as the JVM when dealing with resources and memory management by running collector threads.

Another approach would have consisted in using a one-off thread mechanism whereby, the system would have created a scheduled 
thread possibly using ```ScheduledExecutorService``` class for each call to the pull operation. On the other hand, 
with this approach the system would have probably be more vulnerable and more subject to performance issues as the number 
of potential threads, in a highly concurrent environment, could exponentially grow and make the system unresponsive.


Potential improvements
--

The visibility watcher, while being useful and working well in the background, could I guess be improved in terms of design
and the way it is currently triggered (i.e from the Queue Service class constructor). One possible improvement could have
been to make sure that only one instance of this watcher is triggered for the whole Queue Service instances. However this
approach could have made the visibility cycle longer and more resource consuming to achieve.


As a later discovery and as per my understanding of how SQS Service Queue is working on AWS, the queue count property is
seemingly updated and incremented after each attempt to pull a message. This feature has not been implemented (since not
clearly specified in the instructions document) but could be subject to a future improvement.

Similarly to the above, currently the message-queue API implemented doesn't update the receipt handle while pulling
a message. This could easily implemented in another version by using a similar approach than for the visibility property. 


