/*
 * librdkafka - Apache Kafka C/C++ library
 *
 * Copyright (c) 2014 Magnus Edenhill
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

/**
 * Apache Kafka C/C++ consumer and producer client library.
 *
 * rdkafkacpp.h contains the public C++ API for librdkafka.
 * The API is documented in this file as comments prefixing the class,
 * function, type, enum, define, etc.
 * For more information, see the C interface in rdkafka.h and read the
 * manual in INTRODUCTION.md.
 * The C++ interface is STD C++ '03 compliant and adheres to the
 * Google C++ Style Guide.

 * See_also: For the C interface see rdkafka.h
 *
 */
module rdkafkad;

package import std.string : fromStringz, toStringz;
package import core.stdc.errno;
package import core.stdc.string;
package import core.stdc.stdlib;
package import core.stdc.ctype;
package import core.sys.posix.sys.types;
package import deimos.rdkafka;

public import rdkafkad.config;
public import rdkafkad.handler;
public import rdkafkad.handler.simple_consumer;
public import rdkafkad.handler.kafka_consumer;
public import rdkafkad.handler.producer;
public import rdkafkad.metadata;
public import rdkafkad.topic;

/**
 * librdkafka version
 *
 * Interpreted as hex \c MM.mm.rr.xx:
 *  - MM = Major
 *  - mm = minor
 *  - rr = revision
 *  - xx = pre-release id (0xff is the final release)
 *
 * E.g.: \c 0x000801ff.8.1
 *
 * Note: This value should only be used during compile time,
 *         for runtime checks of version use version()
 */
enum RD_KAFKA_VERSION = 0x00090200;

/**
 * Returns the librdkafka version as integer.
 *
 * See_also: See RD_KAFKA_VERSION for how to parse the integer format.
 */
int version_() nothrow @nogc
{
    return rd_kafka_version();
}

/**
 * Returns the librdkafka version as string.
 */
const(char)[] versionStr() nothrow @nogc
{
    return fromStringz(rd_kafka_version_str);
}

/**
 * Returns a CSV list of the supported debug contexts
 *        for use with Conf::Set("debug", ..).
 */
const(char)[] getDebugContexts() nothrow @nogc
{
    return rd_kafka_get_debug_contexts().fromStringz;
}

/**
 * Wait for all rd_kafka_t objects to be destroyed.
 *
 * 0 if all kafka objects are now destroyed, or -1 if the
 * timeout was reached.
 * Since RdKafka handle deletion is an asynch operation the
 * \p wait_destroyed() function can be used for applications where
 * a clean shutdown is required.
 */
auto waitDestroyed(int timeout_ms)
{
    int ret;
    ret = rd_kafka_wait_destroyed(timeout_ms);
    return ret;
}

/**
 * Error codes.
 *
 * The negative error codes delimited by two underscores
 * (\c _..) denotes errors internal to librdkafka and are
 * displayed as \c \"Local: \<error string..\>\", while the error codes
 * delimited by a single underscore (\c ERR_..) denote broker
 * errors and are displayed as \c \"Broker: \<error string..\>\".
 *
 * See_also: Use err2str() to translate an error code a human readable string
 */
enum ErrorCode
{
    /* Internal errors to rdkafka: */
    /** Begin internal error codes */
    begin = -200,
    /** Received message is incorrect */
    bad_msg = -199,
    /** Bad/unknown compression */
    bad_compression = -198,
    /** Broker is going away */
    destroy = -197,
    /** Generic failure */
    fail = -196,
    /** Broker transport failure */
    transport = -195,
    /** Critical system resource */
    crit_sys_resource = -194,
    /** Failed to resolve broker */
    resolve = -193,
    /** Produced message timed out*/
    msg_timed_out = -192,
    /** Reached the end of the topic+partition queue on
   * the broker. Not really an error. */
    partition_eof = -191,
    /** Permanent: Partition does not exist in cluster. */
    unknown_partition = -190,
    /** File or filesystem error */
    fs = -189,
    /** Permanent: Topic does not exist in cluster. */
    unknown_topic = -188,
    /** All broker connections are down. */
    all_brokers_down = -187,
    /** Invalid argument, or invalid configuration */
    invalid_arg = -186,
    /** Operation timed out */
    timed_out = -185,
    /** Queue is full */
    queue_full = -184,
    /** ISR count < required.acks */
    isr_insuff = -183,
    /** Broker node update */
    node_update = -182,
    /** SSL error */
    ssl = -181,
    /** Waiting for coordinator to become available. */
    wait_coord = -180,
    /** Unknown client group */
    unknown_group = -179,
    /** Operation in progress */
    in_progress = -178,
    /** Previous operation in progress, wait for it to finish. */
    prev_in_progress = -177,
    /** This operation would interfere with an existing subscription */
    existing_subscription = -176,
    /** Assigned partitions (rebalance_cb) */
    assign_partitions = -175,
    /** Revoked partitions (rebalance_cb) */
    revoke_partitions = -174,
    /** Conflicting use */
    conflict = -173,
    /** Wrong state */
    state = -172,
    /** Unknown protocol */
    unknown_protocol = -171,
    /** Not implemented */
    not_implemented = -170,
    /** Authentication failure*/
    authentication = -169,
    /** No stored offset */
    no_offset = -168,
    /** Outdated */
    outdated = -167,
    /** Timed out in queue */
    timed_out_queue = -166,

    /** End internal error codes */
    end = -100,

    /* Kafka broker errors: */
    /** Unknown broker error */
    unknown = -1,
    /** Success */
    no_error,
    /** Offset out of range */
    offset_out_of_range = 1,
    /** Invalid message */
    invalid_msg = 2,
    /** Unknown topic or partition */
    unknown_topic_or_part = 3,
    /** Invalid message size */
    invalid_msg_size = 4,
    /** Leader not available */
    leader_not_available = 5,
    /** Not leader for partition */
    not_leader_for_partition = 6,
    /** Request timed out */
    request_timed_out = 7,
    /** Broker not available */
    broker_not_available = 8,
    /** Replica not available */
    replica_not_available = 9,
    /** Message size too large */
    msg_size_too_large = 10,
    /** StaleControllerEpochCode */
    stale_ctrl_epoch = 11,
    /** Offset metadata string too large */
    offset_metadata_too_large = 12,
    /** Broker disconnected before response received */
    network_exception = 13,
    /** Group coordinator load in progress */
    group_load_in_progress = 14,
    /** Group coordinator not available */
    group_coordinator_not_available = 15,
    /** Not coordinator for group */
    not_coordinator_for_group = 16,
    /** Invalid topic */
    topic_exception = 17,
    /** Message batch larger than configured server segment size */
    record_list_too_large = 18,
    /** Not enough in-sync replicas */
    not_enough_replicas = 19,
    /** Message(s) written to insufficient number of in-sync replicas */
    not_enough_replicas_after_append = 20,
    /** Invalid required acks value */
    invalid_required_acks = 21,
    /** Specified group generation id is not valid */
    illegal_generation = 22,
    /** Inconsistent group protocol */
    inconsistent_group_protocol = 23,
    /** Invalid group.id */
    invalid_group_id = 24,
    /** Unknown member */
    unknown_member_id = 25,
    /** Invalid session timeout */
    invalid_session_timeout = 26,
    /** Group rebalance in progress */
    rebalance_in_progress = 27,
    /** Commit offset data size is not valid */
    invalid_commit_offset_size = 28,
    /** Topic authorization failed */
    topic_authorization_failed = 29,
    /** Group authorization failed */
    group_authorization_failed = 30,
    /** Cluster authorization failed */
    cluster_authorization_failed = 31
}

/**
 * Returns a human readable representation of a kafka error.
 */

string err2str(ErrorCode err) nothrow @nogc
{
    return cast(string)rd_kafka_err2str(cast(rd_kafka_resp_err_t) err).fromStringz;
}

/**
 * Delivery Report callback class
 *
 * The delivery report callback will be called once for each message
 * accepted by Producer::produce() (et.al) with
 * Message::err() set to indicate the result of the produce request.
 *
 * The callback is called when a message is succesfully produced or
 * if librdkafka encountered a permanent failure, or the retry counter for
 * temporary errors has been exhausted.
 *
 * An application must call poll() at regular intervals to
 * serve queued delivery report callbacks.

 */
alias DeliveryReportCb = void delegate(ref Message message) nothrow @nogc;

/**
 * Partitioner callback class
 *
 * Generic partitioner callback class for implementing custom partitioners.
 *
 * See_also: Conf::set() \c "partitioner_cb"
 */
/**
   * Partitioner callback
   *
   * Return the partition to use for \p key in \p topic.
   *
   * The \p msg_opaque is the same \p msg_opaque provided in the
   * Producer::produce() call.
   *
   * Note: \p key may be null or the empty.
   *
   * Must return a value between 0 and \p partition_cnt (non-inclusive).
   *          May return RD_KAFKA_PARTITION_UA (-1) if partitioning failed.
   *
   * See_also: The callback may use Topic::partition_available() to check
   *     if a partition has an active leader broker.
   */
alias PartitionerCb = int delegate(const Topic topic, const(void)[] key,
    int partition_cnt, void* msg_opaque) nothrow @nogc;

/**
 *  Variant partitioner with key pointer
 *
 */
alias PartitionerKeyPointerCb = int delegate(const Topic topic, const(void)* key,
    size_t key_len, int partition_cnt, void* msg_opaque) nothrow @nogc;

/**
 * Event callback class
 *
 * Events are a generic interface for propagating errors, statistics, logs, etc
 * from librdkafka to the application.
 *
 * See_also: Event
 */
alias EventCb = void delegate(ref Event event) nothrow @nogc;

/**
 * Event object class as passed to the EventCb callback.
 */
struct Event
{
nothrow @nogc:
    /** Event type */
    enum Type
    {
        error, /**< Event is an error condition */
        stats, /**< Event is a statistics JSON document */
        log, /**< Event is a log message */
        throttle /**< Event is a throttle level signaling from the broker */
    }

    /** LOG severities (conforms to syslog(3) severities) */
    enum Severity
    {
        emerg,
        alert = 1,
        critical = 2,
        error = 3,
        warning = 4,
        notice = 5,
        info = 6,
        debug_ = 7
    }

    package this(Type type, ErrorCode err, Severity severity, const char* fac, const char* str)
    {
        type_ = type;
        err_ = err;
        severity_ = severity;
        fac_ = fac.fromStringz;
        str_ = str.fromStringz;
    }

    package this(Type type)
    {
        type_ = type;
        err_ = ErrorCode.no_error;
        severity_ = Severity.emerg;
        fac_ = "";
        str_ = "";
    }

    package Type type_;
    package ErrorCode err_;
    package Severity severity_;
    package const(char)[] fac_;
    package const(char)[] str_; /* reused for throttle broker_name */
    package int id_;
    package int throttle_time_;

@property:

    /*
   * Event Accessor methods
   */

    /**
   * The event type
   * Note: Applies to all event types
   */
    Type type() const
    {
        return type_;
    }

    /**
   * Event error, if any.
   * Note: Applies to all event types except throttle
   */
    ErrorCode err() const
    {
        return err_;
    }

    /**
   * Log severity level.
   * Note: Applies to LOG event type.
   */
    Severity severity() const
    {
        return severity_;
    }

    /**
   * Log facility string.
   * Note: Applies to LOG event type.
   */
    const(char)[] fac() const
    {
        return fac_;
    }

    /**
   * Log message string.
   *
   * \c LOG: Log message string.
   * \c STATS: JSON object (as string).
   *
   * Note: Applies to LOG event type.
   */
    const(char)[] str() const
    {
        return str_;
    }

    /**
   * throttle time in milliseconds.
   * Note: Applies to throttle event type.
   */
    int throttleTime() const
    {
        return throttle_time_;
    }

    /**
   * Throttling broker's name.
   * Note: Applies to throttle event type.
   */
    const(char)[] brokerName() const
    {
        if (type_ == Type.throttle)
            return str_;
        else
            return "";
    }

    /**
   * Throttling broker's id.
   * Note: Applies to throttle event type.
   */
    int brokerId() const
    {
        return id_;
    }
}

/**
 * Consume callback class
 */
/**
* The consume callback is used with
*        Consumer::consumeCallback()
*        methods and will be called for each consumed \p message.
*
* The callback interface is optional but provides increased performance.
*/
alias ConsumeCb = void delegate(ref Message message) nothrow @nogc;

/**
 * \b KafkaConsunmer: Rebalance callback class
 */
/**
* Group rebalance callback for use with KafkaConsunmer
*
* Registering a \p rebalance_cb turns off librdkafka's automatic
* partition assignment/revocation and instead delegates that responsibility
* to the application's \p rebalance_cb.
*
* The rebalance callback is responsible for updating librdkafka's
* assignment set based on the two events: ASSIGN_PARTITIONS
* and REVOKE_PARTITIONS but should also be able to handle
* arbitrary rebalancing failures where \p err is neither of those.
* Note: In this latter case (arbitrary error), the application must
*         call unassign() to synchronize state.
*
* Without a rebalance callback this is done automatically by librdkafka
* but registering a rebalance callback gives the application flexibility
* in performing other operations along with the assinging/revocation,
* such as fetching offsets from an alternate location (on assign)
* or manually committing offsets (on revoke).
*/
alias RebalanceCb = void delegate(KafkaConsumer consumer, ErrorCode err,
    ref TopicPartition[] partitions) nothrow @nogc;

/**
 * Offset Commit callback class
 */
/**
* Set offset commit callback for use with consumer groups
*
* The results of automatic or manual offset commits will be scheduled
* for this callback and is served by consume()
* or commitSync()
*
* If no partitions had valid offsets to commit this callback will be called
* with \p err == NO_OFFSET which is not to be considered an error.
*
* The \p offsets list contains per-partition information:
*   - \c topic      The topic committed
*   - \c partition  The partition committed
*   - \c offset:    Committed offset (attempted)
*   - \c err:       Commit error
*/

alias OffsetCommitCb = void delegate(ErrorCode err, ref TopicPartition[] offsets) nothrow @nogc;

/**
 * \b Portability: SocketCb callback class
 *
 */
/**
* Socket callback
*
* The socket callback is responsible for opening a socket
* according to the supplied \p domain, \p type and \p protocol.
* The socket shall be created with \c CLOEXEC set in a racefree fashion, if
* possible.
*
* It is typically not required to register an alternative socket
* implementation
*
* The socket file descriptor or -1 on error (\c errno must be set)
*/
alias SocketCb = int function(int domain, int type, int protocol) nothrow @nogc;


/**
 * Message object
 *
 * This object represents either a single consumed or produced message,
 * or an event (\p err() is set).
 *
 * An application must check Message::err() to see if the
 * object is a proper message (error is ErrorCode.no_error) or a
 * an error event.
 *
 */
struct Message
{
    /**
     * Message timestamp object
     *
     * Represents the number of milliseconds since the epoch (UTC).
     *
     * The Type dictates the timestamp type or origin.
     *
     * Note: Requires Apache Kafka broker version >= 0.10.0
     *
     */
    static struct Timestamp
    {
        enum Type
        {
            not_available, /**< Timestamp not available */
            create_time, /**< Message creation time (source) */
            log_append_time /**< Message log append time (broker) */
        }

        long timestamp; /**< Milliseconds since epoch (UTC). */
        Type type; /**< Timestamp type */
    }


    @disable this(this);
nothrow @nogc:

    bool isNull() @safe pure
    {
        return rkmessage_ is null;
    }

     ~this()
    {
        if (free_rkmessage_ && rkmessage_)
            rd_kafka_message_destroy(cast(rd_kafka_message_t*) rkmessage_);
    }

    this(Topic topic, rd_kafka_message_t* rkmessage, bool dofree = true)
    {
        topic_ = topic;
        rkmessage_ = rkmessage;
        free_rkmessage_ = dofree;
    }

    this(Topic topic, const rd_kafka_message_t* rkmessage)
    {
        assert(rkmessage);
        topic_ = topic;
        rkmessage_ = rkmessage;
        err_ = cast(ErrorCode) rkmessage.err;
    }

    this(rd_kafka_message_t* rkmessage)
    {
        assert(rkmessage);
        rkmessage_ = rkmessage;
        free_rkmessage_ = true;
        err_ = cast(ErrorCode) rkmessage.err;
        if (rkmessage.rkt)
        {
            /* Possibly null */
            topic_ = cast(Topic) rd_kafka_topic_opaque(rkmessage.rkt);
        }
    }

    /* Create errored message */
    this(Topic topic, ErrorCode err)
    {
        topic_ = topic;
        err_ = err;
    }

    /** The error string if object represent an error event,
   *           else an empty string. */
    string errstr() const
    {
        /* FIXME: If there is an error string in payload (for consume_cb)
     *        it wont be shown since 'payload' is reused for errstr
     *        and we cant distinguish between consumer and producer.
     *        For the producer case the payload needs to be the original
     *        payload pointer. */
        return err2str(err_);
    }

    /** The error code if object represents an error event, else 0. */
    ErrorCode err() const
    {
        return err_;
    }

    /** the Topic object for a message (if applicable),
   *            or null if a corresponding Topic object has not been
   *            explicitly created with Topic::create().
   *            In this case use topic_name() instead. */
    const(Topic) topic() const
    {
        return topic_;
    }
    /** Topic name (if applicable, else empty string) */
    const(char)[] topicName() const
    {
        if (rkmessage_.rkt)
            return rd_kafka_topic_name(rkmessage_.rkt).fromStringz;
        else
            return "";
    }
    /** Partition (if applicable) */
    int partition() const
    {
        return rkmessage_.partition;
    }
    /** Message payload (if applicable) */
    const(void)[] payload() const
    {
        return rkmessage_.payload[0 .. rkmessage_.len];
    }

    /** Message key as string (if applicable) */
    const(void)[] key() const
    {
        return (cast(const(char)*) rkmessage_.key)[0 .. rkmessage_.key_len];
    }

    /** Message or error offset (if applicable) */
    long offset() const
    {
        return rkmessage_.offset;
    }

    /** Message timestamp (if applicable) */
    Timestamp timestamp() const
    {
        Timestamp ts;
        rd_kafka_timestamp_type_t tstype;
        ts.timestamp = rd_kafka_message_timestamp(rkmessage_, &tstype);
        ts.type = cast(Timestamp.Type) tstype;
        return ts;
    }

    /** The \p msg_opaque as provided to Producer::produce() */
    const(void)* msgOpaque() const
    {
        return rkmessage_._private;
    }

package:
    Topic topic_;
    const (rd_kafka_message_t)* rkmessage_;
    bool free_rkmessage_;
    ErrorCode err_;
}
