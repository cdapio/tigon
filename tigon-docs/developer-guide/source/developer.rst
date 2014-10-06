.. :author: Cask Data, Inc.
   :description: Guide for developers writing Tigon Applications
   :copyright: Copyright © 2014 Cask Data, Inc.

============================================
Tigon Developer Guide
============================================

Tigon Flows
======================

**Flows** are developer-implemented, real-time stream processors. They
are comprised of one or more `Flowlets`_ that are wired together into a
directed acyclic graph or DAG.

Flowlets pass DataObjects between one another. Each Flowlet is able to
perform custom logic and execute data operations for each individual
data object processed. All data operations happen in a consistent and
durable way.

When processing a single input object, all operations, including the
removal of the object from the input, and emission of data to the
outputs, are executed in a transaction. This provides us with Atomicity,
Consistency, Isolation, and Durability (ACID) properties, and helps
assure a unique and core property of the Flow system: it guarantees
atomic and "exactly-once" processing of each input object by each
Flowlet in the DAG.

A Flow runs as a YARN application and each Flowlet instance runs in its own container.
Each Flowlet in the DAG can have multiple concurrent instances, each consuming a partition
of the Flowlet’s inputs.

To put data into your Flow, you implement a Flowlet to generate or pull the data
from an external source.

The ``Flow`` interface allows you to specify the Flow’s metadata, `Flowlets`_, and
`Flowlet connections <#connection>`_.

To create a Flow, implement ``Flow`` via a ``configure`` method that
returns a ``FlowSpecification`` using ``FlowSpecification.Builder()``::

  class MyExampleFlow implements Flow {
    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("mySampleFlow")
        .setDescription("Flow for showing examples")
        .withFlowlets()
          .add("flowlet1", new MyExampleFlowlet(), 1)
          .add("flowlet2", new MyExampleFlowlet2(), 3)
        .connect()
          .from("flowlet1").to("flowlet2")
        .build();
  }

In this example, the *name*, *description*, *with* (or *without*) Flowlets, and
*connections* are specified before building the Flow. The code above creates a Flow with
two Flowlets; the first named *flowlet1*, a ``MyExampleFlowlet`` with 1 instance, and the
second *flowlet2*, a ``MyExampleFlowlet2`` with 3 instances.

.. _flowlets:

Tigon Flowlets
=========================
**Flowlets**, the basic building blocks of a Flow, represent each
individual processing node within a Flow. Flowlets consume data objects
from their inputs and execute custom logic on each data object, allowing
you to perform data operations as well as emit data objects to the
Flowlet’s outputs. Flowlets specify an ``initialize()`` method, which is
executed at the startup of each instance of a Flowlet before it receives
any data.

The example below shows a Flowlet that reads *Double* values, rounds
them, and emits the results. It has a simple configuration method and
doesn't do anything for initialization or destruction::

  class RoundingFlowlet implements Flowlet {

    @Override
    public FlowletSpecification configure() {
      return FlowletSpecification.Builder.with().
        setName("round").
        setDescription("A rounding Flowlet").
        build();
    }

    @Override
      public void initialize(FlowletContext context) throws Exception {
    }

    @Override
    public void destroy() {
    }

    OutputEmitter<Long> output;
    @ProcessInput
    public void round(Double number) {
      output.emit(Math.round(number));
    }


The most interesting method of this Flowlet is ``round()``, the method
that does the actual processing. It uses an output emitter to send data
to its output. This is the only way that a Flowlet can emit output to
another connected Flowlet::

  OutputEmitter<Long> output;
  @ProcessInput
  public void round(Double number) {
    output.emit(Math.round(number));
  }

Note that the Flowlet declares the output emitter but does not
initialize it. The Flow system initializes and injects its
implementation at runtime.

The method is annotated with @``ProcessInput``—this tells the Flow
system that this method can process input data.

You can overload the process method of a Flowlet by adding multiple
methods with different input types. When an input object comes in, the
Flowlet will call the method that matches the object’s type::

  OutputEmitter<Long> output;

  @ProcessInput
  public void round(Double number) {
    output.emit(Math.round(number));
  }
  @ProcessInput
  public void round(Float number) {
    output.emit((long)Math.round(number));
  }

If you define multiple process methods, a method will be selected based
on the input object’s origin; that is, the name of a Stream or the name
of an output of a Flowlet.

A Flowlet that emits data can specify this name using an annotation on
the output emitter. In the absence of this annotation, the name of the
output defaults to “out”::

  @Output("code")
  OutputEmitter<String> out;

Data objects emitted through this output can then be directed to a
process method of a receiving Flowlet by annotating the method with the
origin name::

  @ProcessInput("code")
  public void tokenizeCode(String text) {
    ... // perform fancy code tokenization
  }

Input Context
-------------
A process method can have an additional parameter, the ``InputContext``.
The input context provides information about the input object, such as
its origin and the number of times the object has been retried. For
example, this Flowlet tokenizes text in a smart way and uses the input
context to decide which tokenizer to use::

  @ProcessInput
  public void tokenize(String text, InputContext context) throws Exception {
    Tokenizer tokenizer;
    // If this failed before, fall back to simple white space
    if (context.getRetryCount() > 0) {
      tokenizer = new WhiteSpaceTokenizer();
    }
    // Is this code? If its origin is named "code", then assume yes
    else if ("code".equals(context.getOrigin())) {
      tokenizer = new CodeTokenizer();
    }
    else {
      // Use the smarter tokenizer
      tokenizer = new NaturalLanguageTokenizer();
    }
    for (String token : tokenizer.tokenize(text)) {
      output.emit(token);
    }
  }

Type Projection
---------------
Flowlets perform an implicit projection on the input objects if they do
not match exactly what the process method accepts as arguments. This
allows you to write a single process method that can accept multiple
**compatible** types. For example, if you have a process method::

  @ProcessInput
  count(String word) {
    ...
  }

and you send data of type ``Long`` to this Flowlet, then that type does
not exactly match what the process method expects. You could now write
another process method for ``Long`` numbers::

  @ProcessInput count(Long number) {
    count(number.toString());
  }

and you could do that for every type that you might possibly want to
count, but that would be rather tedious. Type projection does this for
you automatically. If no process method is found that matches the type
of an object exactly, it picks a method that is compatible with the
object.

In this case, because Long can be converted into a String, it is
compatible with the original process method. Other compatible
conversions are:

- Every primitive type that can be converted to a ``String`` is compatible with
  ``String``.
- Any numeric type is compatible with numeric types that can represent it.
  For example, ``int`` is compatible with ``long``, ``float`` and ``double``,
  and ``long`` is compatible with ``float`` and ``double``, but ``long`` is not
  compatible with ``int`` because ``int`` cannot represent every ``long`` value.
- A byte array is compatible with a ``ByteBuffer`` and vice versa.
- A collection of type A is compatible with a collection of type B,
  if type A is compatible with type B.
  Here, a collection can be an array or any Java ``Collection``.
  Hence, a ``List<Integer>`` is compatible with a ``String[]`` array.
- Two maps are compatible if their underlying types are compatible.
  For example, a ``TreeMap<Integer, Boolean>`` is compatible with a
  ``HashMap<String, String>``.
- Other Java objects can be compatible if their fields are compatible.
  For example, in the following class ``Point`` is compatible with ``Coordinate``,
  because all common fields between the two classes are compatible.
  When projecting from ``Point`` to ``Coordinate``, the color field is dropped,
  whereas the projection from ``Coordinate`` to ``Point`` will leave the ``color`` field
  as ``null``::

    class Point {
      private int x;
      private int y;
      private String color;
    }

    class Coordinates {
      int x;
      int y;
    }

Type projections help you keep your code generic and reusable. They also
interact well with inheritance. If a Flowlet can process a specific
object class, then it can also process any subclass of that class.


Flowlet Method and @Tick Annotation
-----------------------------------
A Flowlet’s method can be annotated with ``@Tick``. Instead of
processing data objects from a Flowlet input, this method is invoked
periodically, without arguments. This can be used, for example, to
generate data, or pull data from an external data source periodically on
a fixed cadence.

In this code snippet from the *CountRandom* example, the ``@Tick``
method in the Flowlet emits random numbers::

  public class RandomSource extends AbstractFlowlet {

    private OutputEmitter<Integer> randomOutput;

    private final Random random = new Random();

    @Tick(delay = 1L, unit = TimeUnit.MILLISECONDS)
    public void generate() throws InterruptedException {
      randomOutput.emit(random.nextInt(10000));
    }
  }

Note: @Tick method calls are serialized; subsequent calls to the tick
method will be made only after the previous @Tick method call has returned.

Connection
----------
There are multiple ways to connect the Flowlets of a Flow. The most
common form is to use the Flowlet name. Because the name of each Flowlet
defaults to its class name, when building the Flow specification you can
simply write::

  .withFlowlets()
    .add(new RandomGenerator())
    .add(new RoundingFlowlet())
  .connect()
    .from("RandomGenerator").to("RoundingFlowlet")

If you have multiple Flowlets of the same class, you can give them explicit names::

  .withFlowlets()
    .add("random", new RandomGenerator())
    .add("generator", new RandomGenerator())
    .add("rounding", new RoundingFlowlet())
  .connect()
    .from("random").to("rounding")
    
    
Batch Execution
---------------
By default, a Flowlet processes a single data object at a time within a single
transaction. To increase throughput, you can also process a batch of data objects within
the same transaction::

  @Batch(100)
  @ProcessInput
  public void process(String words) {
    ...

For the above batch example, the **process** method will be called up to 100 times per
transaction, with different data objects read from the input each time it is called.

If you are interested in knowing when a batch begins and ends, you can use an **Iterator**
as the method argument::

  @Batch(100)
  @ProcessInput
  public void process(Iterator<String> words) {
    ...

In this case, the **process** will be called once per transaction and the **Iterator**
will contain up to 100 data objects read from the input.

Flowlets and Instances
----------------------
You can have one or more instances of any given Flowlet, each consuming a disjoint
partition of each input. You can control the number of instances programmatically via the
`REST interfaces <rest.html>`__ or via the CDAP Console. This enables you
to scale your application to meet capacity at runtime.

In Tigon Standalone, multiple Flowlet instances are run in threads, so in some cases
actual performance may not be improved. However, in the Tigon Distributed,
each Flowlet instance runs in its own Java Virtual Machine (JVM) with independent compute
resources. Scaling the number of Flowlets can improve performance and have a major impact
depending on your implementation.

Partitioning Strategies
-----------------------
As mentioned above, if you have multiple instances of a Flowlet the input queue is
partitioned among the Flowlets. The partitioning can occur in different ways, and each
Flowlet can specify one of these three partitioning strategies:

- **First-in first-out (FIFO):** Default mode. In this mode, every Flowlet instance
  receives the next available data object in the queue. However, since multiple consumers
  may compete for the same data object, access to the queue must be synchronized. This may
  not always be the most efficient strategy.

- **Round-robin:** With this strategy, the number of items is distributed evenly among the
  instances. In general, round-robin is the most efficient partitioning. Though more
  efficient than FIFO, it is not ideal when the application needs to group objects into
  buckets according to business logic. In those cases, hash-based partitioning is
  preferable.

- **Hash-based:** If the emitting Flowlet annotates each data object with a hash key, this
  partitioning ensures that all objects of a given key are received by the same consumer
  instance. This can be useful for aggregating by key, and can help reduce write conflicts
  when writing to HBase in distributed mode.

Let's look at a case where a Hash Partition is required. Suppose we have a Flowlet that counts words::

  public class Counter extends AbstractFlowlet {

    private Map<String, Integer> wordCount = Maps.newHashMap();

    @ProcessInput("wordOut")
    public void process(String word) {
      int count = wordCount.containsKey(word) ? (wordCount.get(word) + 1) : 1;
      wordCount.put(word, count); 
    }
  }

This Flowlet uses the default strategy of FIFO. To increase the throughput when this
Flowlet has many instances, we can specify round-robin partitioning::

  @RoundRobin
  @ProcessInput("wordOut")
  public void process(String word) {
    int count = wordCount.containsKey(word) ? (wordCount.get(word) + 1) : 1;
    wordCount.put(word, count);
  }

Now, if we have three instances of this Flowlet, every instance will receive every third
word. For example, for the sequence of words in the sentence, “I scream, you scream, we
all scream for ice cream”:

- The first instance receives the words: *I scream scream cream*
- The second instance receives the words: *scream we for*
- The third instance receives the words: *you all ice*

The potential problem with this is that the first two instances might
both attempt to increment the counter for the word *scream* at the same time,
leading to a write conflict. To avoid conflicts, we can use hash-based partitioning::

  @HashPartition("wordHash")
  @ProcessInput("wordOut")
  public void process(String word) {
    int count = wordCount.containsKey(word) ? (wordCount.get(word) + 1) : 1;
    wordCount.put(word, count);
  }

Now only one of the Flowlet instances will receive the word *scream*, and there can be no
more write conflicts. Note that in order to use hash-based partitioning, the emitting
Flowlet must annotate each data object with the partitioning key::

  @Output("wordOut")
  private OutputEmitter<String> wordOutput;
  ...
  public void process(StreamEvent event) {
    ...
    // emit the word with the partitioning key name "wordHash"
    wordOutput.emit(word, "wordHash", word.hashCode());
  }

Note that the emitter must use the same name ("wordHash") for the key that the consuming
Flowlet specifies as the partitioning key. If the output is connected to more than one
Flowlet, you can also annotate a data object with multiple hash keys—each consuming
Flowlet can then use different partitioning. This is useful if you want to aggregate by
multiple keys, such as counting purchases by product ID as well as by customer ID.

Partitioning can be combined with batch execution::

  @Batch(100)
  @HashPartition("wordHash")
  @ProcessInput("wordOut")
  public void process(Iterator<String> words) {
     ...

Queues
======

The data flows between Flowlets are implemented through Queues. In the Standalone Mode,
this is implemented through in-memory data structures. In Distributed Mode, it is
implemented using HBase Tables. This provides reliability and fault-tolerance to the Flow
system such that when a Flowlet instances dies, it is respawned and it starts reading
events from the queues from the next event in the queue.


Flow Transaction System
=======================

The Need for Transactions
-------------------------

A Flowlet processes the data objects received on its inputs one at a time. While processing 
a single input object, all operations, including the removal of the data from the input, 
and emission of data to the outputs, are executed in a **transaction**. This provides us 
with ACID—atomicity, consistency, isolation, and durability properties:

- The process method runs under read isolation to ensure that it does not see dirty writes
  (uncommitted writes from concurrent processing) in any of its reads.
  It does see, however, its own writes.

- A failed attempt to process an input object leaves the data in a consistent state;
  it does not leave partial writes behind.

- All writes and emission of data are committed atomically;
  either all of them or none of them are persisted.

- After processing completes successfully, all its writes are persisted in a durable way.

In case of failure, the state of the data is unchanged and processing of the input
object can be reattempted. This ensures "exactly-once" processing of each object.

OCC: Optimistic Concurrency Control
-----------------------------------

Tigon uses *Optimistic Concurrency Control* (OCC) to implement 
transactions. Unlike most relational databases that use locks to prevent conflicting 
operations between transactions, under OCC we allow these conflicting writes to happen. 
When the transaction is committed, we can detect whether it has any conflicts: namely, if 
during the lifetime of the transaction, another transaction committed a write for one of 
the same keys that the transaction has written. In that case, the transaction is aborted 
and all of its writes are rolled back.

In other words: If two overlapping transactions modify the same row, then the transaction 
that commits first will succeed, but the transaction that commits last is rolled back due 
to a write conflict.

Optimistic Concurrency Control is lockless and therefore avoids problems such as idle 
processes waiting for locks, or even worse, deadlocks. However, it comes at the cost of 
rollback in case of write conflicts. We can only achieve high throughput with OCC if the 
number of conflicts is small. It is therefore a good practice to reduce the probability of 
conflicts wherever possible.

Here are some rules to follow for Flows, Flowlets and Procedures:

- Keep transactions short. Tigon attempts to delay the beginning of each
  transaction as long as possible. For instance, if your Flowlet only performs write
  operations, but no read operations, then all writes are deferred until the process
  method returns. They are then performed and transacted, together with the
  removal of the processed object from the input, in a single batch execution.
  This minimizes the duration of the transaction.

- However, if your Flowlet performs a read, then the transaction must
  begin at the time of the read. If your Flowlet performs long-running
  computations after that read, then the transaction runs longer, too,
  and the risk of conflicts increases. It is therefore a good practice
  to perform reads as late in the process method as possible.

- There are two ways to perform an increment: As a write operation that
  returns nothing, or as a read-write operation that returns the incremented
  value. If you perform the read-write operation, then that forces the
  transaction to begin, and the chance of conflict increases. Unless you
  depend on that return value, you should always perform an increment
  only as a write operation.

- Use hash-based partitioning for the inputs of highly concurrent Flowlets
  that perform writes. This helps reduce concurrent writes to the same
  key from different instances of the Flowlet.

Keeping these guidelines in mind will help you write more efficient and faster-performing 
code.

Writing to HBase Transactionally From a Flowlet
-----------------------------------------------

Tigon internally uses Tephra extensively to complete transactional operations.
Tephra can also be leveraged by developers to write to HBase transactionally. To do this, wrap
an *HTable* instance with Tephra’s ``TransactionAwareHTable`` and add it to the Flowlet’s
context.

Here's an example::

  public static final class TransactionalFlowlet extends AbstractFlowlet {

    private OutputEmitter<Integer> intEmitter;
    private int i = 0;

    @Override
    public void initialize(FlowletContext context) throws Exception {
      // Acquire HTable instance
      TransactionAwareHTable txAwareHTable = new TransactionAwareHTable(htable);
      context.addTransactionAware(txAwareHTable);
    }

    @Tick(delay = 1L, unit = TimeUnit.SECONDS)
    public void process() throws Exception {
      Put put = new Put(Bytes.toBytes(“testRow”));
      put.add(Bytes.toBytes(“testFamily”), Bytes.toBytes(“testQualifier”), Bytes.toBytes(i));
      transactionAwareHTable.put(put);

      Integer value = ++i;
      intEmitter.emit(value, "integer", value.hashCode());
    }
  }

Operations performed on the ``TransactionAwareHTable`` instance inside  the ``initialize``, ``destroy``,
and each of the ``process`` methods are committed as a single transaction. Exceptions thrown in any
of these methods will result in a rollback of the entire transaction.

Using TigonSQL
==============

TigonSQL provides an in-memory SQL streaming engine and can perform filtering,
aggregation, and joins of Streams. This can be highly useful for use cases where a large
ingestion rate is required. 

However, it must be noted that as the data in TigonSQL is held in-memory, there is a
possibility of data loss if the Flowlet container or the Stream Engine fails. The
transaction guarantees and the persistence of data comes into play only after the results
of the ``AbstractInputFlowlet`` is emitted and is persisted in HBase Tables through
Queues. A further consideration is that in the current implementation, the instance count
of ``AbstractInputFlowlet`` is limited to a single instance.  

In order to use the TigonSQL library in your flow, you need a Flowlet that extends
``AbstractInputFlowlet``. To use the StreamEngine, implement the ``create method``. The
building blocks of the StreamEngine are the ``StreamSchema``, ``addJSONInput``, and
``addQuery`` methods. 

``StreamSchema`` objects are constructed using the ``StreamSchema`` Builder. These objects
represent the input schema of a Stream, with these fields allowed to be part of the input
schema:

- BOOL
- INT
- LONG
- DOUBLE
- STRING

The Builder’s ``addField`` method takes the name of the field, the field type and the
``SlidingWindowAttribute``. The sliding window attribute is used to annotate that a field is
monotonically increasing or decreasing. A field with this attribute set to
increasing or decreasing might be required for certain SQL queries; for example, "GROUP BY
*increasingField*".

Once one or more ``StreamSchemas`` are created, they are added as an input using the
``addJSONInput`` method. This method takes the name of the input stream and the schema of
the stream. Once the inputs streams have been added, one or more SQL queries can be
defined using an ``addQuery`` method. The ``addQuery`` method takes the name of the query
and the SQL statement.

The output of the SQL queries will be POJOs, whose output class you can define.
The names of the members of the output class should match the names used in the SQL query
statement. In the example given below, ``DataPacket`` is one such POJO class.

In order to process the output of SQL queries, you'll need to annotate the methods
with ``@QueryOutput(<QueryName>)``. You can then choose to process the objects in
that method or emit the object to a subsequent Flowlet. In the example given below,
``emitData`` is a method which is annotated with ``QueryOutput`` and it emits the
``DataPacket`` object to the next Flowlet::

  public class SQLFlowlet extends AbstractInputFlowlet {
      private OutputEmitter<DataPacket> dataEmitter;
      private final Logger LOG = LoggerFactory.getLogger(SQLFlowlet.class);

      @Override
      public void create() {
        setName("Summation");
        setDescription("Sums up the input value over a timewindow");
        StreamSchema schema = new StreamSchema.Builder()
          .addField("timestamp", GDATFieldType.LONG, GDATSlidingWindowAttribute.INCREASING)
          .addField("intStream", GDATFieldType.INT)
          .build();
        addJSONInput("intInput", schema);
        addQuery("sumOut", "SELECT timestamp, SUM(intStream) AS sumValue FROM intInput GROUP BY timestamp");
      }

      @QueryOutput("sumOut")
      public void emitData(DataPacket dataPacket) {
        LOG.info("Emitting data to next flowlet");
        // Each data packet is forwarded to the next flowlet
        dataEmitter.emit(dataPacket);
      }
    }

  class DataPacket {
      // Using the same data type and variable name as specified in the query output
      long timestamp;
      int sumValue;
  }

Ingesting Data into an AbstractInputFlowlet
-------------------------------------------
In order to ingest data into the flowlet, the AbstractInputFlowlet gives couple of
options. One is a HTTP ingestion endpoint; the other is a TCP endpoint. If you run
the Flow in Standalone Mode, the ingestion endpoints are printed out in the log messages
on the console (wrapped for formatting)::

  2014-10-02 16:54:40,401 - INFO  [executor-13:c.c.t.s.f.AbstractInputFlowlet@322] 
    - Announced Data Port tcpPort_intInput - 63537
  2014-10-02 16:54:40,402 - INFO  [executor-13:c.c.t.s.f.AbstractInputFlowlet@322] 
    - Announced Data Port httpPort - 63541

You can ingest data through the HTTP Port using a curl command such as::

  curl -v -X POST http://localhost:63541/v1/tigon/<InputName> -d '{ "data" : [ “12495”, “233“ ] }’

For the example given above, it would then be:: 

  curl -v -X POST http://localhost:63541/v1/tigon/intInput -d '{ "data" : [ “12495”, “233“ ] }’

You can choose to ingest data through either HTTP or TCP endpoints; in the case above, the TCP server is
running on 63537. There is one TCP endpoint for each input stream.

If the Flow is running in Distributed Mode on a cluster, you can use discover using
serviceinfo commands the endpoints.

Optionally, you can provide a runtime arg when you start (``--httpPort=1433``) to give a
port number for the HTTP service. The ``AbstractInputFlowlet`` will attempt to start the
HTTP server on that port; it will fail if it can’t bind to that port. This option may be
useful only in Standalone Mode; in Distributed Mode, you might also need to know the
hostname where the service is running.

TigonSQL, The Query Language
----------------------------
TigonSQL refers both to a library (the In-memory Stream Processing engine
that can perform filtering, aggregation, and joins of data streams) and the language used
by that library.

The Tigon query language, *TigonSQL*, is a pure stream query language with a SQL-like
syntax (being mostly a restriction of SQL).

*TigonSQL* is presented in the `Tigon Architecture Guide 
<architecture#stream-query-language>`__, including the basic concepts with examples.

Details of the language, its theory of operation, quick start guide and complete 
reference can be found in the `Tigon SQL User Manual <_downloads/User_Manual_2014_rev1.html>`__.

For developers who are writing extensions to Tigon SQL, please refer to the
`Tigon SQL Contributor Manual <_downloads/Contributor_Manual_2014_rev1.html>`__.


Best Practices for Developing Applications
==========================================

Initializing Instance Fields
----------------------------
There are three ways to initialize instance fields used in Flowlets:

#. Using the default constructor;
#. Using the ``initialize()`` method of the Flowlets; and
#. Using ``@Property`` annotations.

To initialize using an Property annotation, simply annotate the field definition with 
``@Property``. 

The following example demonstrates the convenience of using ``@Property`` in a 
``WordFilter`` flowlet
that filters out specific words::

  public static class WordFilter extends AbstractFlowlet {
  
    private OutputEmitter<String> out;
  
    @Property
    private final String toFilterOut;
  
    public CountByField(String toFilterOut) {
      this.toFilterOut = toFilterOut;
    }
  
    @ProcessInput()
    public void process(String word) {
      if (!toFilterOut.equals(word)) {
        out.emit(word);
      }
    }
  }


The Flowlet constructor is called with the parameter when the Flow is configured::

  public static class WordCountFlow implements Flow {
    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("WordCountFlow")
        .setDescription("Flow for counting words")
        .withFlowlets().add(new Tokenizer())
                       .add(new WordsFilter("the"))
                       .add(new WordsCounter())
        .connect().fromStream("text").to("Tokenizer")
                  .from("Tokenizer").to("WordsFilter")
                  .from("WordsFilter").to("WordsCounter")
        .build();
    }
  }


At run-time, when the Flowlet is started, a value is injected into the ``toFilterOut`` 
field.

Field types that are supported using the ``@Property`` annotation are primitives,
boxed types (e.g. ``Integer``), ``String`` and ``enum``.


Where to Go Next
================

Now that you're familiar with the components and concepts of Tigon, take a look at:

- `Examples <examples.html>`__, with a complete example demonstrating Tigon.


