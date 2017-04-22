///
module rdkafkad.handler.producer;
import rdkafkad;

import std.datetime: Clock, UTC;

/**
 * Producer
 */
class Producer : Handle
{
    package GlobalConf _conf;
    /**
   * Creates a new Kafka producer handle.
   *
   * \p conf is an optional object that will be used instead of the default
   * configuration.
   * The \p conf object is reusable after this call.
   */
    this(GlobalConf conf)
    {
        _conf = conf;
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

            if (conf.dr_cb_)
            {
                rd_kafka_conf_set_dr_msg_cb(rk_conf, &dr_msg_cb_trampoline);
                this.dr_cb_ = conf.dr_cb_;
            }
        }

        if (null is(rk_ = rd_kafka_new(rd_kafka_type_t.RD_KAFKA_PRODUCER,
                rk_conf, errbuf.ptr, errbuf.sizeof)))
        {
            throw new Exception(errbuf.ptr.fromStringz.idup);
        }
    }

    /++
    Returns: new topic for this producer
    Params:
        topic = topic name
        topicConf = TopicConf, if null `defaultTopicConf` should be setted to the global configuration.
    +/
    Topic newTopic(const(char)[] topic, TopicConf topicConf = null)
    {
        if(!topicConf)
            topicConf = _conf.defaultTopicConf;
        assert(topicConf);
        return new Topic(this, topic, topicConf);
    }

nothrow @nogc:

     ~this()
    {
        if (rk_)
        {
            rd_kafka_destroy(rk_);
        }
    }

    /**
   * Producer::produce() \p msgflags
   *
   * These flags are optional and mutually exclusive.
   */
   enum MsgOpt
   {
        free = 0x1, /**< rdkafka will free(3) \p payload
                                            * when it is done with it. */
        copy = 0x2, /**< the \p payload data will be copied
                                           * and the \p payload pointer will not
                                           * be used by rdkafka after the
                                           * call returns. */
        block = 0x4, /**< Block produce*() on message queue
              *   full.
              *   WARNING:
              *   If a delivery report callback
              *   is used the application MUST
              *   call rd_kafka_poll() (or equiv.)
              *   to make sure delivered messages
              *   are drained from the internal
              *   delivery report queue.
              *   Failure to do so will result
              *   in indefinately blocking on
              *   the produce() call when the
              *   message queue is full.
              */
   }

    nothrow @nogc private static void dr_msg_cb_trampoline(rd_kafka_t* rk,
        const rd_kafka_message_t* rkmessage, void* opaque)
    {
        auto handle = cast(Handle) opaque;
        auto message = Message(null, rkmessage);
        handle.dr_cb_(message);
        message.destroy;
    }

    /**
   * Produce and send a single message to broker.
   *
   * This is an asynch non-blocking API.
   *
   * \p partition is the target partition, either:
   *   - Topic::PARTITION_UA (unassigned) for
   *     automatic partitioning using the topic's partitioner function, or
   *   - a fixed partition (0..N)
   *
   * \p msgflags is zero or more of the following flags OR:ed together:
   *    block - block \p produce*() call if
   *                   \p queue.buffering.max.messages or
   *                   \p queue.buffering.max.kbytes are exceeded.
   *                   Messages are considered in-queue from the point they
   *                   are accepted by produce() until their corresponding
   *                   delivery report callback/event returns.
   *                   It is thus a requirement to call 
   *                   poll() (or equiv.) from a separate
   *                   thread when block is used.
   *                   See WARNING on \c block above.
   *    free - rdkafka will free(3) \p payload when it is done with it.
   *    copy - the \p payload data will be copied and the \p payload
   *               pointer will not be used by rdkafka after the
   *               call returns.
   *
   *  NOTE: free and copy are mutually exclusive.
   *
   *  If the function returns -1 and free was specified, then
   *  the memory associated with the payload is still the caller's
   *  responsibility.
   *
   * \p payload is the message payload of size \p len bytes.
   *
   * \p key is an optional message key, if non-null it
   * will be passed to the topic partitioner as well as be sent with the
   * message to the broker and passed on to the consumer.
   *
   * \p msg_opaque is an optional application-provided per-message opaque
   * pointer that will provided in the delivery report callback (\p dr_cb) for
   * referencing this message.
   *
   * an ErrorCode to indicate success or failure:
   *  - _QUEUE_FULL - maximum number of outstanding messages has been
   *                      reached: \c queue.buffering.max.message
   *
   *  - MSG_SIZE_TOO_LARGE - message is larger than configured max size:
   *                            \c messages.max.bytes
   *
   *  - _UNKNOWN_PARTITION - requested \p partition is unknown in the
   *                           Kafka cluster.
   *
   *  - _UNKNOWN_TOPIC     - topic is unknown in the Kafka cluster.
   */
    ErrorCode produce(Topic topic, int partition, void[] payload,
        const(void)[] key = null,
        long timestamp = Clock.currTime(UTC()).toUnixTime!long,
        int msgflags = MsgOpt.copy,
        void* msg_opaque = null)
    {
        int err;
        //with(rd_kafka_vtype_t)
        //err = rd_kafka_producev(
        //        rk_,
        //        RD_KAFKA_VTYPE_RKT,
        //        topic.rkt_,
        //        RD_KAFKA_VTYPE_PARTITION,
        //        partition,
        //        RD_KAFKA_VTYPE_MSGFLAGS,
        //        msgflags,
        //        RD_KAFKA_VTYPE_VALUE,
        //        payload.ptr,
        //        payload.length,
        //        RD_KAFKA_VTYPE_KEY,
        //        key.ptr,
        //        key.length,
        //        //RD_KAFKA_VTYPE_OPAQUE,
        //        //msg_opaque,
        //        RD_KAFKA_VTYPE_END,
        //        );
        err = rd_kafka_produce(topic.rkt_, partition, msgflags, payload.ptr,
                payload.length, key.ptr, key.length, msg_opaque);
        if (err == -1)
            return cast(ErrorCode) rd_kafka_errno2err(errno);
        return ErrorCode.no_error;
    }

    /**
   * Wait until all outstanding produce requests, et.al, are completed.
   *        This should typically be done prior to destroying a producer instance
   *        to make sure all queued and in-flight produce requests are completed
   *        before terminating.
   *
   * Note: This function will call poll() and thus trigger callbacks.
   *
   * timed_out if \p timeout_ms was reached before all
   *          outstanding requests were completed, else ErrorCode.no_error
   */
    ErrorCode flush(int timeout_ms = 60_000)
    {
        typeof(return) ret;
        ret = cast(ErrorCode) rd_kafka_flush(rk_, timeout_ms);
        return ret;
    }
}
