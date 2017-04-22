///
module rdkafkad.handler.kafka_consumer;
import rdkafkad;

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

nothrow:

    /** Returns the current partition assignment as set by
     *  assign() */
    ErrorCode assignment(ref TopicPartition[] partitions) 
    {
        rd_kafka_topic_partition_list_t* c_parts;
        rd_kafka_resp_err_t err;

        err = rd_kafka_assignment(rk_, &c_parts);
        if (err)
            return cast(ErrorCode) err;

        partitions.length = c_parts.cnt;

        foreach (i, ref p; partitions)
            p = TopicPartition(&c_parts.elems[i]);

        rd_kafka_topic_partition_list_destroy(c_parts);

        return ErrorCode.no_error;

    }

@nogc:

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

        err = rd_kafka_subscribe(rk_, c_topics);

        rd_kafka_topic_partition_list_destroy(c_topics);

        return cast(ErrorCode) err;
    }
    /** Unsubscribe from the current subscription set. */
    auto unsubscribe()
    {
        ErrorCode ret;
        ret = cast(ErrorCode)(rd_kafka_unsubscribe(this.rk_));
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
    auto consume(ref Message msg, int timeout_ms = 10)
    {
        rd_kafka_message_t* rkmessage;

        rkmessage = rd_kafka_consumer_poll(this.rk_, timeout_ms);

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

        err = rd_kafka_assign(rk_, c_parts);

        rd_kafka_topic_partition_list_destroy(c_parts);
        return cast(ErrorCode) err;
    }

    /**
   * Stop consumption and remove the current assignment.
   */
    ErrorCode unassign()
    {
        typeof(return) ret;
        ret = cast(ErrorCode) rd_kafka_assign(rk_, null);
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
        ret = cast(ErrorCode) rd_kafka_commit(rk_, null, 0 /*sync*/ );
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
        ret = cast(ErrorCode) rd_kafka_commit(rk_, null, 1 /*async*/ );
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
        ret = cast(ErrorCode) rd_kafka_commit_message(rk_, message.rkmessage_, 0 /*sync*/ );
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
        ret = cast(ErrorCode) rd_kafka_commit_message(rk_, message.rkmessage_, 1 /*async*/ );
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
        err = rd_kafka_commit(rk_, c_parts, 0);
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
        err = rd_kafka_commit(rk_, c_parts, 1);
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

        err = rd_kafka_committed(rk_, c_parts, timeout_ms);

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

        err = rd_kafka_position(rk_, c_parts);

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
        err = rd_kafka_consumer_close(rk_);
        if (err)
            return cast(ErrorCode) err;

        while (rd_kafka_outq_len(rk_) > 0)
            rd_kafka_poll(rk_, 10);
        rd_kafka_destroy(rk_);

        return cast(ErrorCode) err;
    }
}
