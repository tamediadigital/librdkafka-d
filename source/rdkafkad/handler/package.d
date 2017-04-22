///
module rdkafkad.handler;
import rdkafkad;

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
    auto memberid() const
    {
        char* str;
        str = rd_kafka_memberid(rk_);
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

        ErrorCode error;
        error = cast(ErrorCode)rd_kafka_metadata(rk_, all_topics, topic, &cmetadatap, timeout_ms); 
        if (error)
        {
            throw new Exception(error.err2str);
        }
        return new Metadata(cmetadatap);
    }

@nogc nothrow:

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
    auto queryWatermarkOffsets(const(char)* topic, int partition,
        ref long low, ref long high, int timeout_ms)
    {
        ErrorCode ret;
        ret = cast(ErrorCode) rd_kafka_query_watermark_offsets(rk_, topic,
            partition, &low, &high, timeout_ms);
        return ret;
    }

    package(rdkafkad) rd_kafka_t* rk_;
    /* All Producer and Consumer callbacks must reside in HandleImpl and
     * the opaque provided to rdkafka must be a pointer to HandleImpl, since
     * ProducerImpl and ConsumerImpl classes cannot be safely directly cast to
     * HandleImpl due to the skewed diamond inheritance. */
    package(rdkafkad) EventCb event_cb_;
    package(rdkafkad) SocketCb socket_cb_;
    package(rdkafkad) OpenCb open_cb_;
    package(rdkafkad) DeliveryReportCb dr_cb_;
    package(rdkafkad) PartitionerCb partitioner_cb_;
    package(rdkafkad) PartitionerKeyPointerCb partitioner_kp_cb_;
    package(rdkafkad) RebalanceCb rebalance_cb_;
    package(rdkafkad) OffsetCommitCb offset_commit_cb_;

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
        ret = rd_kafka_poll(rk_, timeout_ms);
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
        ret = rd_kafka_outq_len(rk_);
        return ret;
    }

    /**
     * Convert a list of C partitions to C++ partitions
     */
    nothrow @nogc package static TopicPartition[] c_parts_to_partitions(
        const rd_kafka_topic_partition_list_t* c_parts)
    {
        auto partitions = (cast(TopicPartition*) malloc(c_parts.cnt * TopicPartition.sizeof))[0
            .. c_parts.cnt];
        foreach (i, ref p; partitions)
            partitions[i] = TopicPartition(&c_parts.elems[i]);
        return partitions;
    }

    nothrow @nogc static void free_partition_vector(ref TopicPartition[] v)
    {
        foreach (ref p; v)
            p.destroy;
        free(v.ptr);
        v = null;
    }

    nothrow @nogc package static rd_kafka_topic_partition_list_t* partitions_to_c_parts(const TopicPartition[] partitions)
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
    nothrow @nogc package static void update_partitions_from_c_parts(TopicPartition[] partitions,
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

    nothrow @nogc package static void log_cb_trampoline(const rd_kafka_t* rk, int level,
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

    nothrow @nogc package static void error_cb_trampoline(rd_kafka_t* rk, int err,
        const(char)* reason, void* opaque)
    {
        Handle handle = cast(Handle)(opaque);

        auto event = Event(Event.Type.error, cast(ErrorCode) err,
            Event.Severity.error, null, reason);

        handle.event_cb_(event);
    }

    nothrow @nogc package static void throttle_cb_trampoline(rd_kafka_t* rk,
        const(char)* broker_name, int broker_id, int throttle_time_ms, void* opaque)
    {
        Handle handle = cast(Handle)(opaque);

        auto event = Event(Event.Type.throttle);
        event.str_ = broker_name.fromStringz;
        event.id_ = broker_id;
        event.throttle_time_ = throttle_time_ms;

        handle.event_cb_(event);
    }

    nothrow @nogc package static int stats_cb_trampoline(rd_kafka_t* rk, char* json,
        size_t json_len, void* opaque)
    {
        Handle handle = cast(Handle)(opaque);
        auto event = Event(Event.Type.stats, ErrorCode.no_error, Event.Severity.info,
            null, json);

        handle.event_cb_(event);

        return 0;
    }

    nothrow @nogc package static int socket_cb_trampoline(int domain, int type, int protocol, void* opaque)
    {
        Handle handle = cast(Handle)(opaque);

        return handle.socket_cb_(domain, type, protocol);
    }

    nothrow @nogc package static int open_cb_trampoline(const(char)* pathname, int flags,
        mode_t mode, void* opaque)
    {
        Handle handle = cast(Handle)(opaque);

        return handle.open_cb_(pathname.fromStringz, flags, cast(int)(mode));
    }

    nothrow @nogc package static void rebalance_cb_trampoline(rd_kafka_t* rk,
        rd_kafka_resp_err_t err, rd_kafka_topic_partition_list_t* c_partitions, void* opaque)
    {
        auto handle = cast(KafkaConsumer)(opaque);
        TopicPartition[] partitions = c_parts_to_partitions(c_partitions);

        handle.rebalance_cb_(handle, cast(ErrorCode) err, partitions);

        free_partition_vector(partitions);
    }

    nothrow @nogc package static void offset_commit_cb_trampoline(rd_kafka_t* rk,
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

package:
    rd_kafka_queue_t* queue_;
}
