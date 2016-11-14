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
public import rdkafkad.handlers;
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
int waitDestroyed(int timeout_ms) nothrow @nogc
{
    return rd_kafka_wait_destroyed(timeout_ms);
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
    _BEGIN = -200,
    /** Received message is incorrect */
    _BAD_MSG = -199,
    /** Bad/unknown compression */
    _BAD_COMPRESSION = -198,
    /** Broker is going away */
    _DESTROY = -197,
    /** Generic failure */
    _FAIL = -196,
    /** Broker transport failure */
    _TRANSPORT = -195,
    /** Critical system resource */
    _CRIT_SYS_RESOURCE = -194,
    /** Failed to resolve broker */
    _RESOLVE = -193,
    /** Produced message timed out*/
    _MSG_TIMED_OUT = -192,
    /** Reached the end of the topic+partition queue on
   * the broker. Not really an error. */
    _PARTITION_EOF = -191,
    /** Permanent: Partition does not exist in cluster. */
    _UNKNOWN_PARTITION = -190,
    /** File or filesystem error */
    _FS = -189,
    /** Permanent: Topic does not exist in cluster. */
    _UNKNOWN_TOPIC = -188,
    /** All broker connections are down. */
    _ALL_BROKERS_DOWN = -187,
    /** Invalid argument, or invalid configuration */
    _INVALID_ARG = -186,
    /** Operation timed out */
    _TIMED_OUT = -185,
    /** Queue is full */
    _QUEUE_FULL = -184,
    /** ISR count < required.acks */
    _ISR_INSUFF = -183,
    /** Broker node update */
    _NODE_UPDATE = -182,
    /** SSL error */
    _SSL = -181,
    /** Waiting for coordinator to become available. */
    _WAIT_COORD = -180,
    /** Unknown client group */
    _UNKNOWN_GROUP = -179,
    /** Operation in progress */
    _IN_PROGRESS = -178,
    /** Previous operation in progress, wait for it to finish. */
    _PREV_IN_PROGRESS = -177,
    /** This operation would interfere with an existing subscription */
    _EXISTING_SUBSCRIPTION = -176,
    /** Assigned partitions (rebalance_cb) */
    _ASSIGN_PARTITIONS = -175,
    /** Revoked partitions (rebalance_cb) */
    _REVOKE_PARTITIONS = -174,
    /** Conflicting use */
    _CONFLICT = -173,
    /** Wrong state */
    _STATE = -172,
    /** Unknown protocol */
    _UNKNOWN_PROTOCOL = -171,
    /** Not implemented */
    _NOT_IMPLEMENTED = -170,
    /** Authentication failure*/
    _AUTHENTICATION = -169,
    /** No stored offset */
    _NO_OFFSET = -168,
    /** Outdated */
    _OUTDATED = -167,
    /** Timed out in queue */
    _TIMED_OUT_QUEUE = -166,

    /** End internal error codes */
    _END = -100,

    /* Kafka broker errors: */
    /** Unknown broker error */
    UNKNOWN = -1,
    /** Success */
    NO_ERROR,
    /** Offset out of range */
    OFFSET_OUT_OF_RANGE = 1,
    /** Invalid message */
    INVALID_MSG = 2,
    /** Unknown topic or partition */
    UNKNOWN_TOPIC_OR_PART = 3,
    /** Invalid message size */
    INVALID_MSG_SIZE = 4,
    /** Leader not available */
    LEADER_NOT_AVAILABLE = 5,
    /** Not leader for partition */
    NOT_LEADER_FOR_PARTITION = 6,
    /** Request timed out */
    REQUEST_TIMED_OUT = 7,
    /** Broker not available */
    BROKER_NOT_AVAILABLE = 8,
    /** Replica not available */
    REPLICA_NOT_AVAILABLE = 9,
    /** Message size too large */
    MSG_SIZE_TOO_LARGE = 10,
    /** StaleControllerEpochCode */
    STALE_CTRL_EPOCH = 11,
    /** Offset metadata string too large */
    OFFSET_METADATA_TOO_LARGE = 12,
    /** Broker disconnected before response received */
    NETWORK_EXCEPTION = 13,
    /** Group coordinator load in progress */
    GROUP_LOAD_IN_PROGRESS = 14,
    /** Group coordinator not available */
    GROUP_COORDINATOR_NOT_AVAILABLE = 15,
    /** Not coordinator for group */
    NOT_COORDINATOR_FOR_GROUP = 16,
    /** Invalid topic */
    TOPIC_EXCEPTION = 17,
    /** Message batch larger than configured server segment size */
    RECORD_LIST_TOO_LARGE = 18,
    /** Not enough in-sync replicas */
    NOT_ENOUGH_REPLICAS = 19,
    /** Message(s) written to insufficient number of in-sync replicas */
    NOT_ENOUGH_REPLICAS_AFTER_APPEND = 20,
    /** Invalid required acks value */
    INVALID_REQUIRED_ACKS = 21,
    /** Specified group generation id is not valid */
    ILLEGAL_GENERATION = 22,
    /** Inconsistent group protocol */
    INCONSISTENT_GROUP_PROTOCOL = 23,
    /** Invalid group.id */
    INVALID_GROUP_ID = 24,
    /** Unknown member */
    UNKNOWN_MEMBER_ID = 25,
    /** Invalid session timeout */
    INVALID_SESSION_TIMEOUT = 26,
    /** Group rebalance in progress */
    REBALANCE_IN_PROGRESS = 27,
    /** Commit offset data size is not valid */
    INVALID_COMMIT_OFFSET_SIZE = 28,
    /** Topic authorization failed */
    TOPIC_AUTHORIZATION_FAILED = 29,
    /** Group authorization failed */
    GROUP_AUTHORIZATION_FAILED = 30,
    /** Cluster authorization failed */
    CLUSTER_AUTHORIZATION_FAILED = 31
}

/**
 * Returns a human readable representation of a kafka error.
 */

const(char)[] err2str(ErrorCode err) nothrow @nogc
{
    return rd_kafka_err2str(cast(rd_kafka_resp_err_t) err).fromStringz;
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
        ERROR, /**< Event is an error condition */
        STATS, /**< Event is a statistics JSON document */
        LOG, /**< Event is a log message */
        THROTTLE /**< Event is a throttle level signaling from the broker */
    }

    /** LOG severities (conforms to syslog(3) severities) */
    enum Severity
    {
        EMERG,
        ALERT = 1,
        CRITICAL = 2,
        ERROR = 3,
        WARNING = 4,
        NOTICE = 5,
        INFO = 6,
        DEBUG = 7
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
        err_ = ErrorCode.NO_ERROR;
        severity_ = Severity.EMERG;
        fac_ = "";
        str_ = "";
    }

    package Type type_;
    package ErrorCode err_;
    package Severity severity_;
    package const(char)[] fac_;
    package const(char)[] str_; /* reused for THROTTLE broker_name */
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
   * Note: Applies to all event types except THROTTLE
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
   * Throttle time in milliseconds.
   * Note: Applies to THROTTLE event type.
   */
    int throttleTime() const
    {
        return throttle_time_;
    }

    /**
   * Throttling broker's name.
   * Note: Applies to THROTTLE event type.
   */
    const(char)[] brokerName() const
    {
        if (type_ == Type.THROTTLE)
            return str_;
        else
            return "";
    }

    /**
   * Throttling broker's id.
   * Note: Applies to THROTTLE event type.
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
 * object is a proper message (error is ERR_NO_ERROR) or a
 * an error event.
 *
 */
struct Message
{
    @disable this(this);
nothrow @nogc:
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
        topic_ = topic;
        rkmessage_ = rkmessage;
    }

    this(rd_kafka_message_t* rkmessage)
    {

        rkmessage_ = rkmessage;
        free_rkmessage_ = true;

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
        rkmessage_ = &rkmessage_err_;
        memset(&rkmessage_err_, 0, rkmessage_err_.sizeof);
        rkmessage_err_.err = cast(rd_kafka_resp_err_t)(err);
    }

    /** The error string if object represent an error event,
   *           else an empty string. */
    const(char)[] errstr() const
    {
        /* FIXME: If there is an error string in payload (for consume_cb)
     *        it wont be shown since 'payload' is reused for errstr
     *        and we cant distinguish between consumer and producer.
     *        For the producer case the payload needs to be the original
     *        payload pointer. */
        return rd_kafka_err2str(rkmessage_.err).fromStringz;
    }

    /** The error code if object represents an error event, else 0. */
    ErrorCode err() const
    {
        return cast(ErrorCode) rkmessage_.err;
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
    MessageTimestamp timestamp() const
    {
        MessageTimestamp ts;
        rd_kafka_timestamp_type_t tstype;
        ts.timestamp = rd_kafka_message_timestamp(rkmessage_, &tstype);
        ts.type = cast(MessageTimestamp.Type) tstype;
        return ts;
    }

    /** The \p msg_opaque as provided to Producer::produce() */
    const(void)* msgOpaque() const
    {
        return rkmessage_._private;
    }

package:
    Topic topic_;
    const rd_kafka_message_t* rkmessage_;
    bool free_rkmessage_;
    /* For error signalling by the C++ layer the .._err_ message is
   * used as a place holder and rkmessage_ is set to point to it. */
    rd_kafka_message_t rkmessage_err_;
}
