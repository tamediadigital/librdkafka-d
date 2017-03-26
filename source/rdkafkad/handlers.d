///
module rdkafkad.handlers;
import rdkafkad;
import rdkafkad.iodriver;

/**
 * Base handle, super class for specific clients.
 */
class Handle
{

    /**
    * Returns the client's broker-assigned group member id
    *
    * Note: This currently requires the high-level KafkaConsumer
    *
    * Last assigned member id, or empty string if not currently
    *          a group member.
    */
    string memberid() const nothrow
    {
        char* str;
        mixin(IO!q{
        str = rd_kafka_memberid(rk_);
        });
        string memberid = str.fromStringz.idup;
        if (str)
            rd_kafka_mem_free(cast(rd_kafka_s*) rk_, str);
        return memberid;
    }

    /**
    * Request Metadata from broker.
    *
    * Parameters:
    *  \p all_topics  - if non-zero: request info about all topics in cluster,
    *                   if zero: only request info about locally known topics.
    *  \p only_rkt    - only request info about this topic
    *  \p metadatap   - pointer to hold metadata result.
    *                   The \p *metadatap pointer must be released with \c delete.
    *  \p timeout_ms  - maximum response time before failing.
    *
    * ErrorCode.no_error on success (in which case \p *metadatap
    * will be set), else timed_out on timeout or
    * other error code on error.
    */
    Metadata metadata(bool all_topics, const Topic only_rkt, int timeout_ms = 60_000)
    {
        const rd_kafka_metadata_t* cmetadatap = null;

        rd_kafka_topic_t* topic = only_rkt ? cast(rd_kafka_topic_t*) only_rkt.rkt_ : null;

        mixin(IO!q{
        if (auto error = cast(ErrorCode)rd_kafka_metadata(rk_, all_topics, topic, &cmetadatap, timeout_ms))
        {
            throw new Exception(error.err2str);
        }
        });
        return new Metadata(cmetadatap);
    }

nothrow @nogc:

    package void setCommonConfig(GlobalConf conf)
    {
        rd_kafka_conf_set_opaque(conf.rk_conf_, cast(void*) this);

        if (conf.event_cb_)
        {
            rd_kafka_conf_set_log_cb(conf.rk_conf_, &log_cb_trampoline);
            rd_kafka_conf_set_error_cb(conf.rk_conf_, &error_cb_trampoline);
            rd_kafka_conf_set_throttle_cb(conf.rk_conf_, &throttle_cb_trampoline);
            rd_kafka_conf_set_stats_cb(conf.rk_conf_, &stats_cb_trampoline);
            event_cb_ = conf.event_cb_;
        }

        if (conf.socket_cb_)
        {
            rd_kafka_conf_set_socket_cb(conf.rk_conf_, &socket_cb_trampoline);
            socket_cb_ = conf.socket_cb_;
        }

        version (Windows)
        {
        }
        else if (conf.open_cb_)
        {
            rd_kafka_conf_set_open_cb(conf.rk_conf_, &open_cb_trampoline);
            open_cb_ = conf.open_cb_;
        }

        if (conf.rebalance_cb_)
        {
            rd_kafka_conf_set_rebalance_cb(conf.rk_conf_, &rebalance_cb_trampoline);
            rebalance_cb_ = conf.rebalance_cb_;
        }

        if (conf.offset_commit_cb_)
        {
            rd_kafka_conf_set_offset_commit_cb(conf.rk_conf_, &offset_commit_cb_trampoline);
            offset_commit_cb_ = conf.offset_commit_cb_;
        }
    }

    /**
    * Query broker for low (oldest/beginning)
    *        and high (newest/end) offsets for partition.
    *
    * Offsets are returned in \p *low and \p *high respectively.
    *
    * ErrorCode.no_error on success or an error code on failure.
    */
    ErrorCode queryWatermarkOffsets(const(char)* topic, int partition,
        ref long low, ref long high, int timeout_ms)
    {
        typeof(return) ret;
        mixin(IO!q{
        ret = cast(ErrorCode) rd_kafka_query_watermark_offsets(rk_, topic,
            partition, &low, &high, timeout_ms);
        });
        return ret;
    }

    package rd_kafka_t* rk_;
    /* All Producer and Consumer callbacks must reside in HandleImpl and
     * the opaque provided to rdkafka must be a pointer to HandleImpl, since
     * ProducerImpl and ConsumerImpl classes cannot be safely directly cast to
     * HandleImpl due to the skewed diamond inheritance. */
    package EventCb event_cb_;
    package SocketCb socket_cb_;
    package OpenCb open_cb_;
    package DeliveryReportCb dr_cb_;
    package PartitionerCb partitioner_cb_;
    package PartitionerKeyPointerCb partitioner_kp_cb_;
    package RebalanceCb rebalance_cb_;
    package OffsetCommitCb offset_commit_cb_;

    /** the name of the handle */
    const(char)[] name() const
    {
        return rd_kafka_name(rk_).fromStringz;
    }

    /**
    * Polls the provided kafka handle for events.
    *
    * Events will trigger application provided callbacks to be called.
    *
    * The \p timeout_ms argument specifies the maximum amount of time
    * (in milliseconds) that the call will block waiting for events.
    * For non-blocking calls, provide 0 as \p timeout_ms.
    * To wait indefinately for events, provide -1.
    *
    * Events:
    *   - delivery report callbacks (if an DeliveryCb is configured) [producer]
    *   - event callbacks (if an EventCb is configured) [producer & consumer]
    *
    * Note:  An application should make sure to call poll() at regular
    *          intervals to serve any queued callbacks waiting to be called.
    *
    * Warning: This method MUST NOT be used with the KafkaConsumer,
    *          use its consume() instead.
    *
    * the number of events served.
    */
    int poll(int timeout_ms = 10)
    {
        typeof(return) ret;
        mixin(IO!q{
        ret = rd_kafka_poll(rk_, timeout_ms);
        });
        return ret;
    }

    /**
    *  Returns the current out queue length
    *
    * The out queue contains messages and requests waiting to be sent to,
    * or acknowledged by, the broker.
    */
    int outqLen()
    {
        typeof(return) ret;
        mixin(IO!q{
        ret = rd_kafka_outq_len(rk_);
        });
        return ret;
    }

    /**
     * Convert a list of C partitions to C++ partitions
     */
    private static TopicPartition[] c_parts_to_partitions(
        const rd_kafka_topic_partition_list_t* c_parts)
    {
        auto partitions = (cast(TopicPartition*) malloc(c_parts.cnt * TopicPartition.sizeof))[0
            .. c_parts.cnt];
        foreach (i, ref p; partitions)
            partitions[i] = TopicPartition(&c_parts.elems[i]);
        return partitions;
    }

    static void free_partition_vector(ref TopicPartition[] v)
    {
        foreach (ref p; v)
            p.destroy;
        free(v.ptr);
        v = null;
    }

    private static rd_kafka_topic_partition_list_t* partitions_to_c_parts(const TopicPartition[] partitions)
    {
        rd_kafka_topic_partition_list_t* c_parts = rd_kafka_topic_partition_list_new(cast(int) partitions.length);

        foreach (ref tpi; partitions)
        {
            rd_kafka_topic_partition_t* rktpar = rd_kafka_topic_partition_list_add(c_parts, tpi.topic_, tpi.partition_);
            rktpar.offset = tpi.offset_;
        }

        return c_parts;
    }

    /**
     * @brief Update the application provided 'partitions' with info from 'c_parts'
     */
    private static void update_partitions_from_c_parts(TopicPartition[] partitions,
        const rd_kafka_topic_partition_list_t* c_parts)
    {
        foreach (i; 0 .. c_parts.cnt)
        {
            const rd_kafka_topic_partition_t* p = &c_parts.elems[i];

            /* Find corresponding C++ entry */
            foreach (pp; partitions)
            {
                if (!strcmp(p.topic, pp.topic_) && p.partition == pp.partition_)
                {
                    pp.offset_ = p.offset;
                    pp.err_ = cast(ErrorCode) p.err;
                }
            }
        }
    }

    private static void log_cb_trampoline(const rd_kafka_t* rk, int level,
        const(char)* fac, const(char)* buf)
    {
        if (!rk)
        {
            rd_kafka_log_print(rk, level, fac, buf);
            return;
        }

        void* opaque = rd_kafka_opaque(rk);
        Handle handle = cast(Handle)(opaque);

        if (!handle.event_cb_)
        {
            rd_kafka_log_print(rk, level, fac, buf);
            return;
        }

        auto event = Event(Event.Type.log, ErrorCode.no_error,
            cast(Event.Severity)(level), fac, buf);

        handle.event_cb_(event);
    }

    private static void error_cb_trampoline(rd_kafka_t* rk, int err,
        const(char)* reason, void* opaque)
    {
        Handle handle = cast(Handle)(opaque);

        auto event = Event(Event.Type.error, cast(ErrorCode) err,
            Event.Severity.error, null, reason);

        handle.event_cb_(event);
    }

    private static void throttle_cb_trampoline(rd_kafka_t* rk,
        const(char)* broker_name, int broker_id, int throttle_time_ms, void* opaque)
    {
        Handle handle = cast(Handle)(opaque);

        auto event = Event(Event.Type.throttle);
        event.str_ = broker_name.fromStringz;
        event.id_ = broker_id;
        event.throttle_time_ = throttle_time_ms;

        handle.event_cb_(event);
    }

    private static int stats_cb_trampoline(rd_kafka_t* rk, char* json,
        size_t json_len, void* opaque)
    {
        Handle handle = cast(Handle)(opaque);
        auto event = Event(Event.Type.stats, ErrorCode.no_error, Event.Severity.info,
            null, json);

        handle.event_cb_(event);

        return 0;
    }

    private static int socket_cb_trampoline(int domain, int type, int protocol, void* opaque)
    {
        Handle handle = cast(Handle)(opaque);

        return handle.socket_cb_(domain, type, protocol);
    }

    private static int open_cb_trampoline(const(char)* pathname, int flags,
        mode_t mode, void* opaque)
    {
        Handle handle = cast(Handle)(opaque);

        return handle.open_cb_(pathname.fromStringz, flags, cast(int)(mode));
    }

    private static void rebalance_cb_trampoline(rd_kafka_t* rk,
        rd_kafka_resp_err_t err, rd_kafka_topic_partition_list_t* c_partitions, void* opaque)
    {
        auto handle = cast(KafkaConsumer)(opaque);
        TopicPartition[] partitions = c_parts_to_partitions(c_partitions);

        handle.rebalance_cb_(handle, cast(ErrorCode) err, partitions);

        free_partition_vector(partitions);
    }

    private static void offset_commit_cb_trampoline(rd_kafka_t* rk,
        rd_kafka_resp_err_t err, rd_kafka_topic_partition_list_t* c_offsets, void* opaque)
    {
        Handle handle = cast(Handle)(opaque);
        TopicPartition[] offsets;

        if (c_offsets)
            offsets = c_parts_to_partitions(c_offsets);

        handle.offset_commit_cb_(cast(ErrorCode) err, offsets);

        free_partition_vector(offsets);
    }

    /**
    * Pause producing or consumption for the provided list of partitions.
    *
    * Success or error is returned per-partition in the \p partitions list.
    *
    * ErrorCode::no_error
    *
    * See_also: resume()
    */
    ErrorCode pause(TopicPartition[] partitions)
    {
        rd_kafka_topic_partition_list_t* c_parts;
        rd_kafka_resp_err_t err;

        c_parts = partitions_to_c_parts(partitions);

        err = rd_kafka_pause_partitions(rk_, c_parts);

        if (!err)
            update_partitions_from_c_parts(partitions, c_parts);

        rd_kafka_topic_partition_list_destroy(c_parts);

        return cast(ErrorCode) err;

    }

    /**
    * Resume producing or consumption for the provided list of partitions.
    *
    * Success or error is returned per-partition in the \p partitions list.
    *
    * ErrorCode::no_error
    *
    * See_also: pause()
    */
    ErrorCode resume(TopicPartition[] partitions)
    {
        rd_kafka_topic_partition_list_t* c_parts;
        rd_kafka_resp_err_t err;

        c_parts = partitions_to_c_parts(partitions);

        err = rd_kafka_resume_partitions(rk_, c_parts);

        if (!err)
            update_partitions_from_c_parts(partitions, c_parts);

        rd_kafka_topic_partition_list_destroy(c_parts);

        return cast(ErrorCode) err;
    }

    /**
   * Get last known low (oldest/beginning)
   *        and high (newest/end) offsets for partition.
   *
   * The low offset is updated periodically (if statistics.interval.ms is set)
   * while the high offset is updated on each fetched message set from the
   * broker.
   *
   * If there is no cached offset (either low or high, or both) then
   * OFFSET_INVALID will be returned for the respective offset.
   *
   * Offsets are returned in \p *low and \p *high respectively.
   *
   * ErrorCode.no_error on success or an error code on failure.
   *
   * Note: Shall only be used with an active consumer instance.
   */
    ErrorCode getWatermarkOffsets(const(char)* topic, int partition, ref long low,
        ref long high)
    {
        return cast(ErrorCode) rd_kafka_get_watermark_offsets(rk_, topic,
            partition, &low, &high);
    }
}

/**
 * Queue interface
 *
 * Create a new message queue.  Message queues allows the application
 * to re-route consumed messages from multiple topic+partitions into
 * one single queue point.  This queue point, containing messages from
 * a number of topic+partitions, may then be served by a single
 * consume() method, rather than one per topic+partition combination.
 *
 * See the Consumer::start(), Consumer::consume(), and
 * Consumer::consumeCallback() methods that take a queue as the first
 * parameter for more information.
 */
class Queue
{
nothrow @nogc:
    /**
   * Create Queue object
   */
    this(Handle handle)
    {
        queue_ = rd_kafka_queue_new(handle.rk_);
    }

    ~this()
    {
        rd_kafka_queue_destroy(queue_);
    }

private:
    rd_kafka_queue_t* queue_;
}

/**
 * High-level KafkaConsumer (for brokers 0.9 and later)
 *
 * Note: Requires Apache Kafka >= 0.9.0 brokers
 *
 * Currently supports the \c range and \c roundrobin partition assignment
 * strategies (see \c partition.assignment.strategy)
 */
class KafkaConsumer : Handle
{
    /**
   * Creates a KafkaConsumer.
   *
   * The \p conf object must have \c group.id set to the consumer group to join.
   *
   * Use close() to shut down the consumer.
   *
   * See_also: RebalanceCb
   * See_also: CONFIGURATION.md for \c group.id, \c session.timeout.ms,
   *     \c partition.assignment.strategy, etc.
   */
    this(GlobalConf conf)
    {
        char[512] errbuf = void;
        rd_kafka_conf_t* rk_conf = null;
        size_t grlen;

        if (!conf.rk_conf_)
        {
            throw new Exception("Requires Conf::CONF_GLOBAL object");
        }

        if (rd_kafka_conf_get(conf.rk_conf_, "group.id", null,
                &grlen) != rd_kafka_conf_res_t.RD_KAFKA_CONF_OK || grlen <= 1 /* terminating null only */ )
        {
            throw new Exception("\"group.id\" must be configured");
        }

        this.setCommonConfig(conf);

        rk_conf = rd_kafka_conf_dup(conf.rk_conf_);

        rd_kafka_t* rk;
        if (null is(rk = rd_kafka_new(rd_kafka_type_t.RD_KAFKA_CONSUMER, rk_conf,
                errbuf.ptr, errbuf.sizeof)))
        {
            throw new Exception(errbuf.ptr.fromStringz.idup);
        }

        this.rk_ = rk;

        /* Redirect handle queue to cgrp's queue to provide a single queue point */
        rd_kafka_poll_set_consumer(rk);
    }

    /** Returns the current partition assignment as set by
     *  assign() */
    ErrorCode assignment(ref TopicPartition[] partitions) nothrow
    {
        rd_kafka_topic_partition_list_t* c_parts;
        rd_kafka_resp_err_t err;

        mixin(IO!q{
        err = rd_kafka_assignment(rk_, &c_parts);
        });
        if (err)
            return cast(ErrorCode) err;

        partitions.length = c_parts.cnt;

        foreach (i, ref p; partitions)
            p = TopicPartition(&c_parts.elems[i]);

        rd_kafka_topic_partition_list_destroy(c_parts);

        return ErrorCode.no_error;

    }

nothrow @nogc:

  /**
   * Update the subscription set to \p topics.
   *
   * Any previous subscription will be unassigned and  unsubscribed first.
   *
   * The subscription set denotes the desired topics to consume and this
   * set is provided to the partition assignor (one of the elected group
   * members) for all clients which then uses the configured
   * \c partition.assignment.strategy to assign the subscription sets's
   * topics's partitions to the consumers, depending on their subscription.
   *
   * The result of such an assignment is a rebalancing which is either
   * handled automatically in librdkafka or can be overriden by the application
   * by providing a RebalanceCb.
   *
   * The rebalancing passes the assigned partition set to
   * assign() to update what partitions are actually
   * being fetched by the KafkaConsumer.
   *
   * Regex pattern matching automatically performed for topics prefixed
   * with \c \"^\" (e.g. \c \"^myPfx[0-9]_.*\"
   */
    ErrorCode subscribe(const(char)*[] topics...)
    {
        rd_kafka_topic_partition_list_t* c_topics;
        rd_kafka_resp_err_t err;

        c_topics = rd_kafka_topic_partition_list_new(cast(int) topics.length);

        foreach (t; topics)
            rd_kafka_topic_partition_list_add(c_topics, t, RD_KAFKA_PARTITION_UA);

        mixin(IO!q{
        err = rd_kafka_subscribe(rk_, c_topics);
        });

        rd_kafka_topic_partition_list_destroy(c_topics);

        return cast(ErrorCode) err;
    }
    /** Unsubscribe from the current subscription set. */
    nothrow @nogc ErrorCode unsubscribe()
    {
        typeof(return) ret;
        mixin(IO!q{
        ret = cast(ErrorCode)(rd_kafka_unsubscribe(this.rk_));
        });
        return ret;
    }

    /**
   * Consume message or get error event, triggers callbacks.
   *
   * Will automatically call registered callbacks for any such queued events,
   * including RebalanceCb, EventCb, OffsetCommitCb,
   * etc.
   *
   * Note:  An application should make sure to call consume() at regular
   *          intervals, even if no messages are expected, to serve any
   *          queued callbacks waiting to be called. This is especially
   *          important when a RebalanceCb has been registered as it needs
   *          to be called and handled properly to synchronize internal
   *          consumer state.
   *
   * Note: Application MUST NOT call \p poll() on KafkaConsumer objects.
   *
   * One of:
   *  - proper message (Message::err() is ErrorCode.no_error)
   *  - error event (Message::err() is != ErrorCode.no_error)
   *  - timeout due to no message or event in \p timeout_ms
   *    (Message::err() is timed_out)
   */
   /++
   Params:
        msg = message to fill. Use `msg.err` to check errors.
        timeout_ms = time to to wait if no incomming msgs in queue.
   +/
    nothrow @nogc void consume(ref Message msg, int timeout_ms = 10)
    {
        rd_kafka_message_t* rkmessage;

        mixin(IO!q{
        rkmessage = rd_kafka_consumer_poll(this.rk_, timeout_ms);
        });

        if (!rkmessage)
        {
            msg = Message(null, ErrorCode.timed_out);
            return;
        }

        msg = Message(rkmessage);
    }

    /**
   *  Update the assignment set to \p partitions.
   *
   * The assignment set is the set of partitions actually being consumed
   * by the KafkaConsumer.
   */
    ErrorCode assign(const TopicPartition[] partitions)
    {
        rd_kafka_topic_partition_list_t* c_parts;
        rd_kafka_resp_err_t err;

        c_parts = partitions_to_c_parts(partitions);

        mixin(IO!q{
        err = rd_kafka_assign(rk_, c_parts);
        });

        rd_kafka_topic_partition_list_destroy(c_parts);
        return cast(ErrorCode) err;
    }

    /**
   * Stop consumption and remove the current assignment.
   */
    ErrorCode unassign()
    {
        typeof(return) ret;
        mixin(IO!q{
        ret = cast(ErrorCode) rd_kafka_assign(rk_, null);
        });
        return ret;
    }

    /**
   * Commit offsets for the current assignment.
   *
   * Note: This is the synchronous variant that blocks until offsets
   *         are committed or the commit fails (see return value).
   *
   * Note: If a OffsetCommitCb callback is registered it will
   *         be called with commit details on a future call to
   *         consume()
   *
   * ErrorCode.no_error or error code.
   */
    ErrorCode commitSync()
    {
        typeof(return) ret;
        mixin(IO!q{
        ret = cast(ErrorCode) rd_kafka_commit(rk_, null, 0 /*sync*/ );
        });
        return ret;
    }

    /**
   * Asynchronous version of CommitSync()
   *
   * See_also: KafkaConsummer::commitSync()
   */
    ErrorCode commitAsync()
    {
        typeof(return) ret;
        mixin(IO!q{
        ret = cast(ErrorCode) rd_kafka_commit(rk_, null, 1 /*async*/ );
        });
        return ret;
    }

    /**
   * Commit offset for a single topic+partition based on \p message
   *
   * Note: This is the synchronous variant.
   *
   * See_also: KafkaConsummer::commitSync()
   */
    ErrorCode commitSync(ref Message message)
    {
        typeof(return) ret;
        mixin(IO!q{
        ret = cast(ErrorCode) rd_kafka_commit_message(rk_, message.rkmessage_, 0 /*sync*/ );
        });
        return ret;
    }

    /**
   * Commit offset for a single topic+partition based on \p message
   *
   * Note: This is the asynchronous variant.
   *
   * See_also: KafkaConsummer::commitSync()
   */
    ErrorCode commitAsync(ref Message message)
    {
        typeof(return) ret;
        mixin(IO!q{
        ret = cast(ErrorCode) rd_kafka_commit_message(rk_, message.rkmessage_, 1 /*async*/ );
        });
        return ret;
    }

    /**
   * Commit offsets for the provided list of partitions.
   *
   * Note: This is the synchronous variant.
   */
    ErrorCode commitSync(TopicPartition[] offsets)
    {
        rd_kafka_topic_partition_list_t* c_parts = partitions_to_c_parts(offsets);
        rd_kafka_resp_err_t err;
        mixin(IO!q{
        err = rd_kafka_commit(rk_, c_parts, 0);
        });
        if (!err)
            update_partitions_from_c_parts(offsets, c_parts);
        rd_kafka_topic_partition_list_destroy(c_parts);
        return cast(ErrorCode) err;
    }

    /**
   * Commit offset for the provided list of partitions.
   *
   * Note: This is the asynchronous variant.
   */
    ErrorCode commitAsync(const TopicPartition[] offsets)
    {
        rd_kafka_topic_partition_list_t* c_parts = partitions_to_c_parts(offsets);
        rd_kafka_resp_err_t err;
        mixin(IO!q{
        err = rd_kafka_commit(rk_, c_parts, 1);
        });
        rd_kafka_topic_partition_list_destroy(c_parts);
        return cast(ErrorCode) err;
    }

    /**
   * Retrieve committed offsets for topics+partitions.
   *
   * RD_KAFKA_RESP_ErrorCode.no_error on success in which case the
   *          \p offset or \p err field of each \p partitions' element is filled
   *          in with the stored offset, or a partition specific error.
   *          Else returns an error code.
   */
    ErrorCode committed(TopicPartition[] partitions, int timeout_ms)
    {
        rd_kafka_topic_partition_list_t* c_parts;
        rd_kafka_resp_err_t err;

        c_parts = partitions_to_c_parts(partitions);

        mixin(IO!q{
        err = rd_kafka_committed(rk_, c_parts, timeout_ms);
        });

        if (!err)
        {
            update_partitions_from_c_parts(partitions, c_parts);
        }

        rd_kafka_topic_partition_list_destroy(c_parts);

        return cast(ErrorCode) err;
    }

    /**
   * Retrieve current positions (offsets) for topics+partitions.
   *
   * RD_KAFKA_RESP_ErrorCode.no_error on success in which case the
   *          \p offset or \p err field of each \p partitions' element is filled
   *          in with the stored offset, or a partition specific error.
   *          Else returns an error code.
   */
    ErrorCode position(TopicPartition[] partitions)
    {
        rd_kafka_topic_partition_list_t* c_parts;
        rd_kafka_resp_err_t err;

        c_parts = partitions_to_c_parts(partitions);

        mixin(IO!q{
        err = rd_kafka_position(rk_, c_parts);
        });

        if (!err)
        {
            update_partitions_from_c_parts(partitions, c_parts);
        }

        rd_kafka_topic_partition_list_destroy(c_parts);

        return cast(ErrorCode) err;

    }

    /**
   * Close and shut down the proper.
   *
   * This call will block until the following operations are finished:
   *  - Trigger a local rebalance to void the current assignment
   *  - Stop consumption for current assignment
   *  - Commit offsets
   *  - Leave group
   *
   * The maximum blocking time is roughly limited to session.timeout.ms.
   *
   * Note: Callbacks, such as RebalanceCb and
   *         OffsetCommitCb, etc, may be called.
   *
   * Note: The consumer object must later be freed with \c delete
   */
    ErrorCode close()
    {
        rd_kafka_resp_err_t err;
        mixin(IO!q{
        err = rd_kafka_consumer_close(rk_);
        });
        if (err)
            return cast(ErrorCode) err;

        mixin(IO!q{
        while (rd_kafka_outq_len(rk_) > 0)
            rd_kafka_poll(rk_, 10);
        rd_kafka_destroy(rk_);
        });

        return cast(ErrorCode) err;
    }
}

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

        mixin(IO!q{
        rk_ = rd_kafka_new(rd_kafka_type_t.RD_KAFKA_CONSUMER,
                rk_conf, errbuf.ptr, errbuf.sizeof);
        });
        if (null is rk_)
        {
            throw new Exception(errbuf.ptr.fromStringz.idup);
        }
    }

nothrow @nogc:

     ~this()
    {
        mixin(IO!q{
        rd_kafka_destroy(rk_);
        });
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
        mixin(IO!q{
        err = rd_kafka_consume_start(topic.rkt_, partition, offset);
        });
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
        mixin(IO!q{
        err = rd_kafka_consume_stop(topic.rkt_, partition);
        });
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
        mixin(IO!q{
        err = rd_kafka_seek(topic.rkt_, partition, offset, timeout_ms);
        });
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

        mixin(IO!q{
        rkmessage = rd_kafka_consume(topic.rkt_, partition, timeout_ms);
        });
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
        mixin(IO!q{
        rkmessage = rd_kafka_consume_queue(queue.queue_, timeout_ms);
        });
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
        return rd_kafka_consume_callback(topic.rkt_, partition, timeout_ms,
            &ConsumerCallback.consume_cb_trampoline, &context);
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
        return rd_kafka_consume_callback_queue(queue.queue_, timeout_ms,
            &ConsumerQueueCallback.consume_cb_trampoline, &context);
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

    private static void dr_msg_cb_trampoline(rd_kafka_t* rk,
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
        const(void)[] key = null, int msgflags = MsgOpt.copy, void* msg_opaque = null)
    {
        int err;
        mixin(IO!q{
        err = rd_kafka_produce(topic.rkt_, partition, msgflags, payload.ptr,
                payload.length, key.ptr, key.length, msg_opaque);
        });
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
        mixin(IO!q{
        ret = cast(ErrorCode) rd_kafka_flush(rk_, timeout_ms);
        });
        return ret;
    }
}
