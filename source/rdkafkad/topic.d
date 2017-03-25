///
module rdkafkad.topic;
import rdkafkad;

/**
 * Topic+Partition
 *
 * This is a generic type to hold a single partition and various
 * information about it.
 *
 * Is typically used with std::vector<TopicPartition*> to provide
 * a list of partitions for different operations.
 */
struct TopicPartition
{
    package const(char)* topic_;
    package int partition_;
    package long offset_;
    package ErrorCode err_;

    void toString(in void delegate(const(char)[]) sink) const
    {
        sink(topic);
        import std.format;
        sink.formattedWrite("[%s]", partition_);
    }

   /**
   * Create topic+partition object for \p topic and \p partition.
   *
   * Use \c delete to deconstruct.
   */

    nothrow:

    /// ditto
    this(const(char)[] topic, int partition)
    {
        this(topic.toStringz, partition);
    }

    @nogc:

    this(const(char)* topic, int partition)
    {
        topic_ = topic;
        partition_ = partition;
        offset_ = Offset.invalid;
        err_ = ErrorCode.no_error;
    }


    package this(const rd_kafka_topic_partition_t* c_part)
    {
        topic_ = c_part.topic;
        partition_ = c_part.partition;
        offset_ = c_part.offset;
        err_ = cast(ErrorCode) c_part.err;
        // FIXME: metadata
    }

    /** partition id */
    int partition() @property
    {
        return partition_;
    }
    /** topic name */
    const(char)[] topic() const @property
    {
        return topic_.fromStringz;
    }

    /** offset (if applicable) */
    long offset() @property
    {
        return offset_;
    }

    /** Set offset */
    void offset(long offset) @property
    {
        offset_ = offset;
    }

    /** error code (if applicable) */
    ErrorCode err() @property
    {
        return err_;
    }
}

/** Special offsets */
enum Offset
{
    beginning = -2, /**< Consume from beginning */
    end = -1, /**< Consume from end */
    stored = -1000, /**< Use offset storage */
    invalid = -1001, /**< Invalid offset */
}

/**
 * Topic handle
 *
 */
class Topic
{
    /**
   * Creates a new topic handle for topic named \p topic_str
   *
   * \p conf is an optional configuration for the topic  that will be used
   * instead of the default topic configuration.
   * The \p conf object is reusable after this call.
   *
   * the new topic handle or null on error (see \p errstr).
   */
    this(Handle base, const(char)[] topic_str, TopicConf conf)
    {
        rd_kafka_topic_conf_t* rkt_conf;

        if (!conf)
            rkt_conf = rd_kafka_topic_conf_new();
        else /* Make a copy of conf struct to allow Conf reuse. */
            rkt_conf = rd_kafka_topic_conf_dup(conf.rkt_conf_);

        /* Set topic opaque to the topic so that we can reach our topic object
     * from whatever callbacks get registered.
     * The application itself will not need these opaques since their
     * callbacks are class based. */
        rd_kafka_topic_conf_set_opaque(rkt_conf, cast(void*) this);

        if (conf)
        {
            if (conf.partitioner_cb_)
            {
                rd_kafka_topic_conf_set_partitioner_cb(rkt_conf, &partitioner_cb_trampoline);
                this.partitioner_cb_ = conf.partitioner_cb_;
            }
            else if (conf.partitioner_kp_cb_)
            {
                rd_kafka_topic_conf_set_partitioner_cb(rkt_conf, &partitioner_kp_cb_trampoline);
                this.partitioner_kp_cb_ = conf.partitioner_kp_cb_;
            }
        }
        rkt_ = rd_kafka_topic_new(base.rk_, topic_str.toStringz(), rkt_conf);
        if (!rkt_)
        {
            auto msg = err2str(cast(ErrorCode)rd_kafka_errno2err(errno));
            rd_kafka_topic_conf_destroy(rkt_conf);
            throw new Exception(msg);
        }
    }

nothrow @nogc:

     ~this()
    {
        if (rkt_)
            rd_kafka_topic_destroy(rkt_);
    }

    package rd_kafka_topic_t* rkt_;
    package PartitionerCb partitioner_cb_;
    package PartitionerKeyPointerCb partitioner_kp_cb_;

    /**
   * Unassigned partition.
   *
   * The unassigned partition is used by the producer API for messages
   * that should be partitioned using the configured or default partitioner.
   */
    enum int unassignedPartition = -1;

    package static nothrow @nogc int partitioner_cb_trampoline(
        const rd_kafka_topic_t* rkt, const(void)* keydata, size_t keylen,
        int partition_cnt, void* rkt_opaque, void* msg_opaque)
    {
        auto topic = cast(Topic) rkt_opaque;
        auto key = (cast(const(char)*) keydata)[0 .. keylen];
        return topic.partitioner_cb_(topic, key, partition_cnt, msg_opaque);
    }

    package static nothrow @nogc int partitioner_kp_cb_trampoline(
        const rd_kafka_topic_t* rkt, const(void)* keydata, size_t keylen,
        int partition_cnt, void* rkt_opaque, void* msg_opaque)
    {
        auto topic = cast(Topic) rkt_opaque;
        return topic.partitioner_kp_cb_(topic, keydata, keylen, partition_cnt, msg_opaque);
    }

    /** the topic name */
    final const(char)[] name() const
    {
        return rd_kafka_topic_name(rkt_).fromStringz;
    }

    /**
   * true if \p partition is available for the topic (has leader).
   * Warning: \b MUST \b ONLY be called from within a
   *          PartitionerCb callback.
   */
    final bool partitionAvailable(int partition) const
    {
        return cast(bool) rd_kafka_topic_partition_available(rkt_, partition);
    }

    /**
   * Store offset \p offset for topic partition \p partition.
   * The offset will be committed (written) to the offset store according
   * to \p auto.commit.interval.ms.
   *
   * Note: This API should only be used with the simple Consumer,
   *         not the high-level KafkaConsumer.
   * Note: \c auto.commit.enable must be set to \c false when using this API.
   *
   * ErrorCodde.no_error on success or an error code on error.
   */
    final ErrorCode offsetStore(int partition, long offset)
    {
        return cast(ErrorCode) rd_kafka_offset_store(rkt_, partition, offset);
    }
}
