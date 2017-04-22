///
module rdkafkad.handler.simple_consumer;
import rdkafkad;

/**
 * Simple Consumer (legacy)
 *
 * A simple non-balanced, non-group-aware, consumer.
 */
class Consumer : Handle
{
    /**
   * Creates a new Kafka consumer handle.
   *
   * \p conf is an optional object that will be used instead of the default
   * configuration.
   * The \p conf object is reusable after this call.
   *
   * the new handle on success or null on error in which case
   * \p errstr is set to a human readable error message.
   */
    this(GlobalConf conf)
    {
        char[512] errbuf = void;
        rd_kafka_conf_t* rk_conf = null;

        if (conf)
        {
            if (!conf.rk_conf_)
            {
                throw new Exception("Requires Conf::CONF_GLOBAL object");
            }

            this.setCommonConfig(conf);

            rk_conf = rd_kafka_conf_dup(conf.rk_conf_);
        }

        rk_ = rd_kafka_new(rd_kafka_type_t.RD_KAFKA_CONSUMER,
                rk_conf, errbuf.ptr, errbuf.sizeof);
        if (null is rk_)
        {
            throw new Exception(errbuf.ptr.fromStringz.idup);
        }
    }

nothrow @nogc:

     ~this()
    {
        rd_kafka_destroy(rk_);
    }

static:

    /**
   * Start consuming messages for topic and \p partition
   * at offset \p offset which may either be a proper offset (0..N)
   * or one of the the special offsets: \p OFFSET_BEGINNING or \p OFFSET_END.
   *
   * rdkafka will attempt to keep \p queued.min.messages (config property)
   * messages in the local queue by repeatedly fetching batches of messages
   * from the broker until the threshold is reached.
   *
   * The application shall use one of the \p ...consume*() functions
   * to consume messages from the local queue, each kafka message being
   * represented as a `Message ` object.
   *
   * \p ...start() must not be called multiple times for the same
   * topic and partition without stopping consumption first with
   * \p ...stop().
   *
   * an ErrorCode to indicate success or failure.
   */
    ErrorCode start(Topic topic, int partition, long offset, Queue queue)
    {
        int err;
        err = rd_kafka_consume_start(topic.rkt_, partition, offset);
        if (err == -1)
            return cast(ErrorCode)(rd_kafka_errno2err(errno));
        return ErrorCode.no_error;
    }

    /**
   * Stop consuming messages for topic and \p partition, purging
   *        all messages currently in the local queue.
   *
   * The application needs to be stop all consumers before destroying
   * the Consumer handle.
   *
   * an ErrorCode to indicate success or failure.
   */
    ErrorCode stop(Topic topic, int partition)
    {
        int err;
        err = rd_kafka_consume_stop(topic.rkt_, partition);
        if (err == -1)
            return cast(ErrorCode)(rd_kafka_errno2err(errno));
        return ErrorCode.no_error;
    }

    /**
   * Seek consumer for topic+partition to \p offset which is either an
   *        absolute or logical offset.
   *
   * If \p timeout_ms is not 0 the call will wait this long for the
   * seek to be performed. If the timeout is reached the internal state
   * will be unknown and this function returns `timed_out`.
   * If \p timeout_ms is 0 it will initiate the seek but return
   * immediately without any error reporting (e.g., async).
   *
   * This call triggers a fetch queue barrier flush.
   *
   * an ErrorCode to indicate success or failure.
   */
    ErrorCode seek(Topic topic, int partition, long offset, int timeout_ms)
    {
        int err;
        err = rd_kafka_seek(topic.rkt_, partition, offset, timeout_ms);
        if (err == -1)
            return cast(ErrorCode)(rd_kafka_errno2err(errno));
        return ErrorCode.no_error;
    }

    /**
   * Consume a single message from \p topic and \p partition.
   *
   * \p timeout_ms is maximum amount of time to wait for a message to be
   * received.
   * Consumer must have been previously started with \p ...start().
   *
   * a Message object, the application needs to check if message
   * is an error or a proper message Message::err() and checking for
   * \p ErrorCode.no_error.
   *
   * The message object must be destroyed when the application is done with it.
   *
   * Errors (in Message::err()):
   *  - timed_out - \p timeout_ms was reached with no new messages fetched.
   *  - PARTITION_EOF - End of partition reached, not an error.
   */
    void consume(Topic topic, int partition, int timeout_ms, ref Message msg)
    {
        rd_kafka_message_t* rkmessage;

        rkmessage = rd_kafka_consume(topic.rkt_, partition, timeout_ms);
        if (!rkmessage)
            msg = Message(topic, cast(ErrorCode) rd_kafka_errno2err(errno));

        msg = Message(topic, rkmessage);
    }

    /**
   * Consume a single message from the specified queue.
   *
   * \p timeout_ms is maximum amount of time to wait for a message to be
   * received.
   * Consumer must have been previously started on the queue with
   * \p ...start().
   *
   * a Message object, the application needs to check if message
   * is an error or a proper message \p Message.err() and checking for
   * \p ErrorCode.no_error.
   *
   * The message object must be destroyed when the application is done with it.
   *
   * Errors (in Message::err()):
   *   - timed_out - \p timeout_ms was reached with no new messages fetched
   *
   * Note that Message.topic() may be nullptr after certain kinds of
   * errors, so applications should check that it isn't null before
   * dereferencing it.
   */
    void consume(Queue queue, int timeout_ms, ref Message msg)
    {
        rd_kafka_message_t* rkmessage;
        rkmessage = rd_kafka_consume_queue(queue.queue_, timeout_ms);
        if (!rkmessage)
            msg = Message(null, cast(ErrorCode) rd_kafka_errno2err(errno));
        /*
       * Recover our Topic from the topic conf's opaque field, which we
       * set in Topic::create() for just this kind of situation.
       */
        void* opaque = rd_kafka_topic_opaque(rkmessage.rkt);
        Topic topic = cast(Topic)(opaque);

        msg = Message(topic, rkmessage);
    }

    /* Helper struct for `consume_callback'.
   * Encapsulates the values we need in order to call `rd_kafka_consume_callback'
   * and keep track of the C++ callback function and `opaque' value.
   */
    private static struct ConsumerCallback
    {
        /* This function is the one we give to `rd_kafka_consume_callback', with
     * the `opaque' pointer pointing to an instance of this struct, in which
     * we can find the C++ callback and `cb_data'.
     */
        static nothrow @nogc void consume_cb_trampoline(rd_kafka_message_t* msg, void* opaque)
        {
            ConsumerCallback* instance = cast(ConsumerCallback*) opaque;
            Message message = Message(instance.topic, msg, false  /*don't free*/ );
            instance.cb_cls(message);
        }

        Topic topic;
        ConsumeCb cb_cls;
    }

    /**
   * Consumes messages from \p topic and \p partition, calling
   *        the provided callback for each consumed messsage.
   *
   * \p consumeCallback() provides higher throughput performance
   * than \p consume().
   *
   * \p timeout_ms is the maximum amount of time to wait for one or
   * more messages to arrive.
   *
   * The provided \p consume_cb instance has its \p consume_cb function
   * called for every message received.
   *
   * The \p opaque argument is passed to the \p consume_cb as \p opaque.
   *
   * the number of messages processed or -1 on error.
   *
   * See_also: Consumer::consume()
   */
    int consumeCallback(Topic topic, int partition, int timeout_ms, ConsumeCb consume_cb)
    {
        auto context = ConsumerCallback(topic, consume_cb);
        int ret;
        ret = rd_kafka_consume_callback(topic.rkt_, partition, timeout_ms,
            &ConsumerCallback.consume_cb_trampoline, &context);
        return ret;
    }

    /* Helper struct for `consume_callback' with a Queue.
   * Encapsulates the values we need in order to call `rd_kafka_consume_callback'
   * and keep track of the C++ callback function and `opaque' value.
   */
    private static struct ConsumerQueueCallback
    {
        /* This function is the one we give to `rd_kafka_consume_callback', with
     * the `opaque' pointer pointing to an instance of this struct, in which
     * we can find the C++ callback and `cb_data'.
     */
        static nothrow @nogc void consume_cb_trampoline(rd_kafka_message_t* msg, void* opaque)
        {
            ConsumerQueueCallback* instance = cast(ConsumerQueueCallback*) opaque;
            /*
       * Recover our Topic from the topic conf's opaque field, which we
       * set in Topic::create() for just this kind of situation.
       */
            void* topic_opaque = rd_kafka_topic_opaque(msg.rkt);
            Topic topic = cast(Topic) topic_opaque;
            Message message = Message(topic, msg, false  /*don't free*/ );
            instance.cb_cls(message);
        }

        ConsumeCb cb_cls;
    }

    /**
   * Consumes messages from \p queue, calling the provided callback for
   *        each consumed messsage.
   *
   * See_also: Consumer::consumeCallback()
   */

    int consumeCallback(Queue queue, int timeout_ms, ConsumeCb consume_cb)
    {
        auto context = ConsumerQueueCallback(consume_cb);
        int ret;
        ret = rd_kafka_consume_callback_queue(queue.queue_, timeout_ms,
            &ConsumerQueueCallback.consume_cb_trampoline, &context);
        return ret;
    }

    /**
   * Converts an offset into the logical offset from the tail of a topic.
   *
   * \p offset is the (positive) number of items from the end.
   *
   * the logical offset for message \p offset from the tail, this value
   *          may be passed to Consumer::start, et.al.
   * Note: The returned logical offset is specific to librdkafka.
   */
    long offsetTail(long offset)
    {
        return RD_KAFKA_OFFSET_TAIL(offset);
    }
}
