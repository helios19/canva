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
    boolean runVisibilityMonitor = true;

    InMemoryQueueService queueService = new InMemoryQueueService(visibilityTimeoutInSecs, runVisibilityMonitor);
        
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
    boolean addShutdownHook = true;

    // file queue service
    FileQueueService queueService = new FileQueueService(baseDirPath, visibilityTimeoutInSecs, addShutdownHook);
    
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

The solution implemented tries to follows some of the common characteristics of AWS SQS Queue Service, as per
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

- BDD: Each Queue Service public method has been implemented along with their related tests covering the expected
functional behaviors
- SOLID principles: Single Responsibility, Opened/Closed, Liskov substitution, Interface segregation, Dependency inversion
- 10-50-600: Not more than 10 classes per package, methods are not exceeding 50 lines and classes length are 
below 600 lines
- Documentation and comments on all public methods and classes


#### Concurrency In Multi-Thread and Inter-Process Environment

In order to achieve concurrency in a multi-thread and inter-process environment the following patterns have been applied:


###### Atomic operation

The JDK platform provides some classes supporting atomic operations through the CAS feature provided by most of the operating systems.
This type of concurrency mechanisms is preferable over the use of Locks mainly for performance reasons.

The implemented message-queue API takes advantage of this type of classes to offer thread-safety while running multiple
consumers and producers concurrently. In particular, the In-memory message-queue service uses the ```AtomicLong.compareAndSet```
method to atomically update the visibility timestamp of a message while being accessed by multiple threads.

###### Concurrent data structures

Again here, the JDK gathers a raft of concurrent structures aiming at facilitating the development of thread-safe program
by implementing some of the most efficient paradigms and mechanisms such as lock-free, non-blocking, optimistic-locking
or standard pessimistic-locking.

The system implemented uses some of these data structures such as:
            
- ConcurrentLinkedQueue: one of the best suited non-blocking structure when it comes to implement a multi-threads consumers
producers API. The In-memory Queue Service implementation uses it to store the messages.
    
- ConcurrentHashMap: Provides bucket-locking mechanism as opposed to its non-concurrent counterparts. The File-based
Queue Service makes use of it to store a list of Reentrant lock per queue name.

- CopyOnWriteArrayList: Another concurrent list implementation providing bucket locking. The File-based Queue Service 
uses this structure to save the list of lock file created (and not yet deleted) by the system to let the 
```FileLockShutdownHook``` thread remove them if still around when the program exit abruptly. 


###### Immutability

Immutability is probably one of the best way to deal with concurrency and greatly helps any system to scale out.
The message-queue API implemented uses immutability mainly to protect the internal state of its messages from any external
intervention (e.g from the API callers). Indeed, when pulling a message from the queue, the system returns an immutable
version of the message (```ImmutableMessageQueue``` class) preventing any external changes to be applied on the messages stored 
in the queue.

Note that the ```MessageQueue``` class itself is not immutable (currently only the AtomicLong visibility property can change)
as it provides some useful methods to update the visibility timestamp in a atomic manner.


###### Inter-process locking using mkdir

As described in the blog post (https://engineering.canva.com/2015/03/25/hermeticity/), in order to achieve inter-process 
safety, the system uses a .lock folder created using mkdir which leverages on the atomicity of this command on POSIX filesystem.
All message operations provided by the File-based Queue Service class are synchronized using a .lock folder created with mkdir.

In addition to the .lock folder, and in order to properly achieve thread-safety while dealing with pull/push/delete methods, 
a map of ```ReentrantLock``` instance per queue name is used to exclusively let a thread access a particular
queue file. This approach of lock instance per queue provides a good compromise between performance and state consistency,
as the thread blocking lock is applied per queue, leaving other threads free to access concurrently any other queue without blocking.



#### Visibility Timeout Feature

In order to implement the visibility timeout feature and as per the description of this functionality from the AWS SQS 
online documentation, two different approaches have been utilized for each Queue Service implementation classes.

*In-memory Queue Service*

As described above, the In-memory Queue Service implementation class is using concurrent data structure 
(namely ConcurrentLinkedQueue and ConcurentHashMap) and Atomic variables to maintain consistency and achieve 
thread-safety when being called by multiple concurrent producers/consumers. This approach provides some obvious 
benefits in terms of performance due to the non-blocking nature of these data structures and the CAS OS feature being used
by atomic object variables. However, this comes, also sometimes, at the expense of crafting complex mutable 
operations in an atomic way.

In particular, for the visibility timeout feature, an easy way to implement it would have consisted in 
letting the Queue Service public methods the task to check the timestamp (greater or lower than current) 
and update it to current if the timeout has expired (done by the pull method) which means the message 
is visible again. However, in order to make these two tasks (check/update) thread-safe, in a multi-thread 
environment, they would have been executed in an atomic way. One could have used here a standard 
blocking lock strategy, maybe by using ReentrantLock class or the like. This would have worked, but it 
also would have slowed down the system.

The Atomic variable classes available on the JDK, offer some quite useful atomic methods, one of these method is the
compareAndSet which has been used in the In-memory Queue Service to accomplish the check/update tasks 
atomically. Unfortunately, as per the current specification, this type of Atomic classes don't provide yet
any methods that would compare a condition and act upon it to update a variable (a sort of checkConditionAndSet
methods). The compareAndSet method works only when the initial value is known by the caller which is not the
case in regards to the visibility timestamp which could be set to any value.

On the other hand, one value that is known to be a valid and deterministic timestamp value is 0, being the initial
value set by the "push" method. Thus, in order to keep using the atomic variable approach, and avoid reverting
to a blocking strategy, the decision has been made to delegate the visibility check to an external
watcher thread that will check periodically the timestamp of each message in the queue and for those that have
expired, reset them to 0.

This way, the pull method has now a deterministic way to check and update the timestamp 
of messages (0 being the only valid value of a visible message as far as the pull method is concerned). This check
and update tasks can now happen using the compareAndSet atomic method. 
As explained above, a single watcher thread running in the background, will reset to 0 the timestamp of messages 
whenever their invisibility period has elapsed. This approach is comparable to how the JVM is dealing with resource
and memory management, delegating to another class of collector threads the dirty task to clean up the mess!
The ```VisibilityMessageMonitor``` thread class, used by the In-memory Queue Service, has been implemented 
for similar purposes, but in this case, specifically to deal with visibility timeout reset.

Although, this visibility watcher is automatically running when instantiating the In-memory Queue Service, a 
caller still has the possibility to turn it off by either calling the ```close()``` method or by setting 
the ```runVisibilityWatcher``` property to false when invoking the Queue Service constructor.

*File-based Queue Service*

As opposed to the approach described above for the In-memory Queue Service, a different approach has been adopted
for the File-based Queue Service. The main reason for this is primarily due to the double locking scheme used
by this service class whereby a process lock along with another thread lock per queue are applied to maintain 
consistency in a multi-process and multi-thread environment.
Nevertheless, using this double lock strategy defeats from the start the usefulness of concurrent data structure or atomic 
operations, as when a thread acquired the lock on a particular queue, it is not only blocking the other threads
trying to access the same queue, but also the other processes competing to update the same file messages.

So, for the File-based Queue Service, the decision has been made to let the pull method run the check and update 
operations (check the visibility timestamp and update it if expired), as the access of a queue is done in a mutually 
exclusive way.

One could argue about the reason behind the use a thread blocking lock per queue on top of the inter-process file
lock. It turns out that the lock file strategy in itself doesn't truly provide a safe way to achieve consistency 
for multi-threads concurrently running the mkdir command, as they could still end up in a race condition. 
 

Trade-offs and limitations
--

The watcher approach for implementing the visibility timeout in the In-memory Queue Service, is a simple yet 
effective solution to address the issue of maintaining the visibility state for each message. 
Other runtime platforms have come up with similar solutions, such as the JVM when dealing with resources and 
memory management by running collector threads.

This solution, while being quite effective in terms of performance, presents some disavantages such as 

    - Having an additional delay between timeout period and the time the visibility watcher is running
     before a invisible message becomes visible again
    - Having an extra thread running in the background could have some impacts in the total resource 
    consumption of the system

However, given the current instructions of this exercise, using a visibility monitor that allows the use
of concurrent data structures and atomic operations, seems to outweight the above drawbacks.  


As explained in the previous paragraph, the File-based Queue Service is using a double locking scheme made up
of file lock and thread lock to safely maintain the visibility state of messages for multi-vms and multi-threads
running concurrently. This blocking lock approach has definitely some obvious impacts on the performance of the system 
but is still providing a safe way to maintain consistently the state of the visibility property across messages.


Potential improvements
--

The visibility watcher, while being useful and working well in the background, could I guess be improved in terms of design
and the way it is currently triggered (i.e from the Queue Service class constructor). One possible improvement could have
been to make sure that only one instance of this watcher is triggered for the whole Queue Service instances. However this
approach could have made the visibility cycle longer and more resource consuming.

The double locking scheme used by the File-based Queue Service class could have been improved by combining this 
scheme with a record locking strategy instead which could have help this service to better perform. 

As a later discovery and as per my understanding of how SQS Service Queue is working on AWS, the queue count property is
seemingly updated and incremented after each attempt to pull a message. This feature has not been implemented and 
could be subject to a future improvement.

Similarly to the above, currently the message-queue API implemented doesn't update the receipt handle while pulling
a message. This could easily implemented in another version by using a similar approach than for the visibility property. 


