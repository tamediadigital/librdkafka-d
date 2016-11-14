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

import std.string : fromStringz, toStringz;
import core.stdc.errno;
import core.stdc.string;
import core.stdc.stdlib;
import core.stdc.ctype;
import core.sys.posix.sys.types;
import deimos.rdkafka;

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

    private this(Type type, ErrorCode err, Severity severity, const char* fac, const char* str)
    {
        type_ = type;
        err_ = err;
        severity_ = severity;
        fac_ = fac.fromStringz;
        str_ = str.fromStringz;
    }

    private this(Type type)
    {
        type_ = type;
        err_ = ErrorCode.NO_ERROR;
        severity_ = Severity.EMERG;
        fac_ = "";
        str_ = "";
    }

    private Type type_;
    private ErrorCode err_;
    private Severity severity_;
    private const(char)[] fac_;
    private const(char)[] str_; /* reused for THROTTLE broker_name */
    private int id_;
    private int throttle_time_;

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
 * \b Portability: OpenCb callback class
 *
 */
/**
   * Open callback
   * The open callback is responsible for opening the file specified by
   * \p pathname, using \p flags and \p mode.
   * The file shall be opened with \c CLOEXEC set in a racefree fashion, if
   * possible.
   *
   * It is typically not required to register an alternative open implementation
   *
   * Note: Not currently available on native Win32
   */
alias OpenCb = int function(const(char)[] path, int flags, int mode) nothrow @nogc;

///
class ConfException : Exception
{
    /**
    * Conf::Set() result code
    */
    enum Result
    {
        UNKNOWN = -2, /**< Unknown configuration property */
        INVALID = -1, /**< Invalid configuration value */
        OK /**< Configuration property was succesfully set */
    }
    ///
    Result result;
    ///
    this(Result result, string msg, string file = __FILE__,
        uint line = cast(uint) __LINE__, Throwable next = null) pure nothrow @nogc @safe
    {
        super(msg, file, line, next);
        this.result = result;
    }
}

/**
 * Configuration interface
 *
 * Holds either global or topic configuration that are passed to
 * Consumer::create(), Producer::create(),
 * create(), etc.
 *
 * See_also: CONFIGURATION.md for the full list of supported properties.
 */
interface Conf
{
    /**
     * Set configuration property \p name to value \p value.
     * OK on success, else writes a human readable error
     *          description to \p errstr on error.
     */
    void opIndexAssign(in char[] value, in char[] name);

    /** Query single configuration value
   *  OK if the property was set previously set and
   *           returns the value in \p value. */
    string opIndex(in char[] name) const;

    string[] dump();
}

class GlobalConf : Conf
{

    /** Dump configuration names and values to list containing
   *         name,value tuples */
    string[] dump()
    {
        size_t cnt;
        auto arrc = rd_kafka_conf_dump(rk_conf_, &cnt);
        auto arr = new string[cnt];
        foreach (size_t i; 0 .. cnt)
            arr[i] = arrc[i].fromStringz.idup;
        rd_kafka_conf_dump_free(arrc, cnt);
        return arr;
    }

    /**
     * Set configuration property \p name to value \p value.
     * OK on success, else writes a human readable error
     *          description to \p errstr on error.
     */
    void opIndexAssign(in char[] value, in char[] name)
    {
        char[512] errbuf = void;

        auto res = rd_kafka_conf_set(this.rk_conf_, name.toStringz(),
            value.toStringz(), errbuf.ptr, errbuf.sizeof);
        if (res != rd_kafka_conf_res_t.RD_KAFKA_CONF_OK)
            throw new ConfException(cast(ConfException.Result) res,
                errbuf.ptr.fromStringz.idup);
    }

    /** Query single configuration value
    *  OK if the property was set previously set and
    *           returns the value in \p value. */
    string opIndex(in char[] name) const
    {
        size_t size;

        rd_kafka_conf_res_t res = rd_kafka_conf_res_t.RD_KAFKA_CONF_OK;
        res = rd_kafka_conf_get(rk_conf_, name.toStringz(), null, &size);
        if (res != rd_kafka_conf_res_t.RD_KAFKA_CONF_OK)
            throw new ConfException(cast(ConfException.Result) res, "can not get config");

        char[] value = new char[size];
        res = rd_kafka_conf_get(rk_conf_, name.toStringz(), value.ptr, &size);
        if (res != rd_kafka_conf_res_t.RD_KAFKA_CONF_OK)
            throw new ConfException(cast(ConfException.Result) res, "can not get config");
        return cast(string) value;
    }

nothrow @nogc:

     ~this() nothrow @nogc
    {
        if (rk_conf_)
            rd_kafka_conf_destroy(rk_conf_);
    }

    /**
    * Create configuration object
    */
    this()
    {
        rk_conf_ = rd_kafka_conf_new();
    }

    private DeliveryReportCb dr_cb_;
    private EventCb event_cb_;
    private SocketCb socket_cb_;
    private OpenCb open_cb_;
    private RebalanceCb rebalance_cb_;
    private OffsetCommitCb offset_commit_cb_;
    private rd_kafka_conf_t* rk_conf_;

    /++
    +/
    void drCb(DeliveryReportCb cb) @property
    {
        dr_cb_ = cb;
    }
    /++
    +/
    void eventCb(EventCb cb) @property
    {
        event_cb_ = cb;
    }
    /++
    +/
    void socketCb(SocketCb cb) @property
    {
        socket_cb_ = cb;
    }
    /++
    +/
    void openCb(OpenCb cb) @property
    {
        open_cb_ = cb;
    }
    /++
    +/
    void rebalanceCb(RebalanceCb cb) @property
    {
        rebalance_cb_ = cb;
    }
    /++
    +/
    void offsetCommitCb(OffsetCommitCb cb) @property
    {
        offset_commit_cb_ = cb;
    }

    /** Use with \p name = \c \"default_topic_conf\"
     *
     * Sets the default topic configuration to use for for automatically
     * subscribed topics.
     *
     * See_also: subscribe()
     */
    void defaultTopicConf(const TopicConf topic_conf) @property
    {
        rd_kafka_conf_set_default_topic_conf(rk_conf_,
            rd_kafka_topic_conf_dup(topic_conf.rkt_conf_));
    }
}

class TopicConf : Conf
{
    /**
     * Set configuration property \p name to value \p value.
     * OK on success, else writes a human readable error
     *          description to \p errstr on error.
     */
    void opIndexAssign(in char[] value, in char[] name)
    {
        rd_kafka_conf_res_t res;
        char[512] errbuf = void;

        res = rd_kafka_topic_conf_set(this.rkt_conf_, name.toStringz(),
            value.toStringz(), errbuf.ptr, errbuf.sizeof);

        if (res != rd_kafka_conf_res_t.RD_KAFKA_CONF_OK)
            throw new ConfException(cast(ConfException.Result) res,
                errbuf.ptr.fromStringz.idup);
    }

    /** Query single configuration value
   *  OK if the property was set previously set and
   *           returns the value in \p value. */
    string opIndex(in char[] name) const
    {
        size_t size;
        rd_kafka_conf_res_t res = rd_kafka_conf_res_t.RD_KAFKA_CONF_OK;
        res = rd_kafka_topic_conf_get(rkt_conf_, name.toStringz(), null, &size);
        if (res != rd_kafka_conf_res_t.RD_KAFKA_CONF_OK)
            throw new ConfException(cast(ConfException.Result) res, "can not get config");
        char[] value = new char[size];
        res = rd_kafka_topic_conf_get(rkt_conf_, name.toStringz(), value.ptr, &size);
        if (res != rd_kafka_conf_res_t.RD_KAFKA_CONF_OK)
            throw new ConfException(cast(ConfException.Result) res, "can not get config");
        return cast(string) value;
    }

    /** Dump configuration names and values to list containing
     *         name,value tuples */
    string[] dump()
    {
        size_t cnt;
        auto arrc = rd_kafka_topic_conf_dump(rkt_conf_, &cnt);
        auto arr = new string[cnt];
        foreach (size_t i; 0 .. cnt)
            arr[i] = arrc[i].fromStringz.idup;
        rd_kafka_conf_dump_free(arrc, cnt);
        return arr;
    }

nothrow @nogc:

     ~this()
    {
        if (rkt_conf_)
            rd_kafka_topic_conf_destroy(rkt_conf_);
    }

    /**
    * Create configuration object
    */
    this()
    {
        rkt_conf_ = rd_kafka_topic_conf_new();
    }

    private PartitionerCb partitioner_cb_;
    private PartitionerKeyPointerCb partitioner_kp_cb_;
    private rd_kafka_topic_conf_t* rkt_conf_;

    /++
    +/
    void partitionerCb(PartitionerCb cb) @property
    {
        partitioner_cb_ = cb;
    }
    /++
    +/
    void partitionerKpCb(PartitionerKeyPointerCb cb) @property
    {
        partitioner_kp_cb_ = cb;
    }
}

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
        char* str = rd_kafka_memberid(rk_);
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
    * ERR_NO_ERROR on success (in which case \p *metadatap
    * will be set), else TIMED_OUT on timeout or
    * other error code on error.
    */
    ErrorCode metadata(bool all_topics, const Topic only_rkt, ref Metadata metadata, int timeout_ms) nothrow
    {
        const rd_kafka_metadata_t* cmetadatap = null;

        rd_kafka_topic_t* topic = only_rkt ? cast(rd_kafka_topic_t*) only_rkt.rkt_ : null;

        const rd_kafka_resp_err_t rc = rd_kafka_metadata(rk_, all_topics,
            topic, &cmetadatap, timeout_ms);

        metadata = new Metadata(cmetadatap);

        return cast(ErrorCode) rc;
    }

nothrow @nogc:

    private void setCommonConfig(GlobalConf conf)
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
    * ERR_NO_ERROR on success or an error code on failure.
    */
    ErrorCode queryWatermarkOffsets(const(char)* topic, int partition,
        ref long low, ref long high, int timeout_ms)
    {
        return cast(ErrorCode) rd_kafka_query_watermark_offsets(rk_, topic,
            partition, &low, &high, timeout_ms);
    }

    private rd_kafka_t* rk_;
    /* All Producer and Consumer callbacks must reside in HandleImpl and
     * the opaque provided to rdkafka must be a pointer to HandleImpl, since
     * ProducerImpl and ConsumerImpl classes cannot be safely directly cast to
     * HandleImpl due to the skewed diamond inheritance. */
    private EventCb event_cb_;
    private SocketCb socket_cb_;
    private OpenCb open_cb_;
    private DeliveryReportCb dr_cb_;
    private PartitionerCb partitioner_cb_;
    private PartitionerKeyPointerCb partitioner_kp_cb_;
    private RebalanceCb rebalance_cb_;
    private OffsetCommitCb offset_commit_cb_;

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
    int poll(int timeout_ms)
    {
        return rd_kafka_poll(rk_, timeout_ms);
    }

    /**
    *  Returns the current out queue length
    *
    * The out queue contains messages and requests waiting to be sent to,
    * or acknowledged by, the broker.
    */
    int outqLen()
    {
        return rd_kafka_outq_len(rk_);
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

        auto event = Event(Event.Type.LOG, ErrorCode.NO_ERROR,
            cast(Event.Severity)(level), fac, buf);

        handle.event_cb_(event);
    }

    private static void error_cb_trampoline(rd_kafka_t* rk, int err,
        const(char)* reason, void* opaque)
    {
        Handle handle = cast(Handle)(opaque);

        auto event = Event(Event.Type.ERROR, cast(ErrorCode) err,
            Event.Severity.ERROR, null, reason);

        handle.event_cb_(event);
    }

    private static void throttle_cb_trampoline(rd_kafka_t* rk,
        const(char)* broker_name, int broker_id, int throttle_time_ms, void* opaque)
    {
        Handle handle = cast(Handle)(opaque);

        auto event = Event(Event.Type.THROTTLE);
        event.str_ = broker_name.fromStringz;
        event.id_ = broker_id;
        event.throttle_time_ = throttle_time_ms;

        handle.event_cb_(event);
    }

    private static int stats_cb_trampoline(rd_kafka_t* rk, char* json,
        size_t json_len, void* opaque)
    {
        Handle handle = cast(Handle)(opaque);
        auto event = Event(Event.Type.STATS, ErrorCode.NO_ERROR, Event.Severity.INFO,
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
    * ErrorCode::NO_ERROR
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
    * ErrorCode::NO_ERROR
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
   * ERR_NO_ERROR on success or an error code on failure.
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
    private const(char)* topic_;
    private int partition_;
    private long offset_;
    private ErrorCode err_;

    void toString(in void delegate(const(char)[]) sink) const
    {
        sink(topic);
        import std.format;
        sink.formattedWrite("[%s]", partition_);
    }

nothrow @nogc:

    /**
   * Create topic+partition object for \p topic and \p partition.
   *
   * Use \c delete to deconstruct.
   */
    nothrow this(const(char)* topic, int partition)
    {
        topic_ = topic;
        partition_ = partition;
        offset_ = Topic.OFFSET_INVALID;
        err_ = ErrorCode.NO_ERROR;
    }

    /// ditto
    this(const(char)* topic, int partition)
    {
        topic_ = topic;
        partition_ = partition;
        offset_ = Topic.OFFSET_INVALID;
        err_ = ErrorCode.NO_ERROR;
    }

    private this(const rd_kafka_topic_partition_t* c_part)
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
            auto errstr = rd_kafka_err2str(rd_kafka_errno2err(errno));
            rd_kafka_topic_conf_destroy(rkt_conf);
            throw new Exception(cast(string) errstr.fromStringz);
        }
    }

nothrow @nogc:

     ~this()
    {
        if (rkt_)
            rd_kafka_topic_destroy(rkt_);
    }

    private rd_kafka_topic_t* rkt_;
    private PartitionerCb partitioner_cb_;
    private PartitionerKeyPointerCb partitioner_kp_cb_;

    /**
   * Unassigned partition.
   *
   * The unassigned partition is used by the producer API for messages
   * that should be partitioned using the configured or default partitioner.
   */
    enum int PARTITION_UA = -1;

    /** Special offsets */
    enum long OFFSET_BEGINNING = -2; /**< Consume from beginning */
    enum long OFFSET_END = -1; /**< Consume from end */
    enum long OFFSET_STORED = -1000; /**< Use offset storage */
    enum long OFFSET_INVALID = -1001; /**< Invalid offset */

    private static nothrow @nogc int partitioner_cb_trampoline(
        const rd_kafka_topic_t* rkt, const(void)* keydata, size_t keylen,
        int partition_cnt, void* rkt_opaque, void* msg_opaque)
    {
        auto topic = cast(Topic) rkt_opaque;
        auto key = (cast(const(char)*) keydata)[0 .. keylen];
        return topic.partitioner_cb_(topic, key, partition_cnt, msg_opaque);
    }

    private static nothrow @nogc int partitioner_kp_cb_trampoline(
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
   * ERR_NO_ERROR on success or an error code on error.
   */
    final ErrorCode offsetStore(int partition, long offset)
    {
        return cast(ErrorCode) rd_kafka_offset_store(rkt_, partition, offset);
    }
}

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

struct MessageTimestamp
{
    enum Type
    {
        MSG_TIMESTAMP_NOT_AVAILABLE, /**< Timestamp not available */
        MSG_TIMESTAMP_CREATE_TIME, /**< Message creation time (source) */
        MSG_TIMESTAMP_LOG_APPEND_TIME /**< Message log append time (broker) */
    }

    long timestamp; /**< Milliseconds since epoch (UTC). */
    Type type; /**< Timestamp type */
}

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

private:
    Topic topic_;
    const rd_kafka_message_t* rkmessage_;
    bool free_rkmessage_;
    /* For error signalling by the C++ layer the .._err_ message is
   * used as a place holder and rkmessage_ is set to point to it. */
    rd_kafka_message_t rkmessage_err_;
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

        if (0 != (err = rd_kafka_assignment(rk_, &c_parts)))
            return cast(ErrorCode) err;

        partitions.length = c_parts.cnt;

        foreach (i, ref p; partitions)
            p = TopicPartition(&c_parts.elems[i]);

        rd_kafka_topic_partition_list_destroy(c_parts);

        return ErrorCode.NO_ERROR;

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

        err = rd_kafka_subscribe(rk_, c_topics);

        rd_kafka_topic_partition_list_destroy(c_topics);

        return cast(ErrorCode) err;

    }
    /** Unsubscribe from the current subscription set. */
    nothrow @nogc ErrorCode unsubscribe()
    {
        return cast(ErrorCode)(rd_kafka_unsubscribe(this.rk_));
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
   *  - proper message (Message::err() is ERR_NO_ERROR)
   *  - error event (Message::err() is != ERR_NO_ERROR)
   *  - timeout due to no message or event in \p timeout_ms
   *    (Message::err() is TIMED_OUT)
   */
    nothrow @nogc void consume(int timeout_ms, ref Message msg)
    {
        rd_kafka_message_t* rkmessage;

        rkmessage = rd_kafka_consumer_poll(this.rk_, timeout_ms);

        if (!rkmessage)
        {
            msg = Message(null, ErrorCode._TIMED_OUT);
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
        return cast(ErrorCode) rd_kafka_assign(rk_, null);
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
   * ERR_NO_ERROR or error code.
   */
    ErrorCode commitSync()
    {
        return cast(ErrorCode) rd_kafka_commit(rk_, null, 0 /*sync*/ );

    }

    /**
   * Asynchronous version of CommitSync()
   *
   * See_also: KafkaConsummer::commitSync()
   */
    ErrorCode commitAsync()
    {
        return cast(ErrorCode) rd_kafka_commit(rk_, null, 1 /*async*/ );
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
        return cast(ErrorCode) rd_kafka_commit_message(rk_, message.rkmessage_, 0 /*sync*/ );
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
        return cast(ErrorCode) rd_kafka_commit_message(rk_, message.rkmessage_, 1 /*async*/ );
    }

    /**
   * Commit offsets for the provided list of partitions.
   *
   * Note: This is the synchronous variant.
   */
    ErrorCode commitSync(TopicPartition[] offsets)
    {
        rd_kafka_topic_partition_list_t* c_parts = partitions_to_c_parts(offsets);
        rd_kafka_resp_err_t err = rd_kafka_commit(rk_, c_parts, 0);
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
        rd_kafka_resp_err_t err = rd_kafka_commit(rk_, c_parts, 1);
        rd_kafka_topic_partition_list_destroy(c_parts);
        return cast(ErrorCode) err;
    }

    /**
   * Retrieve committed offsets for topics+partitions.
   *
   * RD_KAFKA_RESP_ERR_NO_ERROR on success in which case the
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
   * RD_KAFKA_RESP_ERR_NO_ERROR on success in which case the
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

        if (null is(rk_ = rd_kafka_new(rd_kafka_type_t.RD_KAFKA_CONSUMER,
                rk_conf, errbuf.ptr, errbuf.sizeof)))
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
        if (rd_kafka_consume_start(topic.rkt_, partition, offset) == -1)
            return cast(ErrorCode)(rd_kafka_errno2err(errno));
        return ErrorCode.NO_ERROR;
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
        if (rd_kafka_consume_stop(topic.rkt_, partition) == -1)
            return cast(ErrorCode)(rd_kafka_errno2err(errno));
        return ErrorCode.NO_ERROR;
    }

    /**
   * Seek consumer for topic+partition to \p offset which is either an
   *        absolute or logical offset.
   *
   * If \p timeout_ms is not 0 the call will wait this long for the
   * seek to be performed. If the timeout is reached the internal state
   * will be unknown and this function returns `TIMED_OUT`.
   * If \p timeout_ms is 0 it will initiate the seek but return
   * immediately without any error reporting (e.g., async).
   *
   * This call triggers a fetch queue barrier flush.
   *
   * an ErrorCode to indicate success or failure.
   */
    ErrorCode seek(Topic topic, int partition, long offset, int timeout_ms)
    {
        if (rd_kafka_seek(topic.rkt_, partition, offset, timeout_ms) == -1)
            return cast(ErrorCode)(rd_kafka_errno2err(errno));
        return ErrorCode.NO_ERROR;
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
   * \p ERR_NO_ERROR.
   *
   * The message object must be destroyed when the application is done with it.
   *
   * Errors (in Message::err()):
   *  - TIMED_OUT - \p timeout_ms was reached with no new messages fetched.
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
   * \p ERR_NO_ERROR.
   *
   * The message object must be destroyed when the application is done with it.
   *
   * Errors (in Message::err()):
   *   - TIMED_OUT - \p timeout_ms was reached with no new messages fetched
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
    /**
   * Creates a new Kafka producer handle.
   *
   * \p conf is an optional object that will be used instead of the default
   * configuration.
   * The \p conf object is reusable after this call.
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

nothrow @nogc:

     ~this()
    {
        if (rk_)
        {
            rd_kafka_destroy(rk_);
        }
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
   * Producer::produce() \p msgflags
   *
   * These flags are optional and mutually exclusive.
   */
   enum Msg
   {
        FREE = 0x1, /**< rdkafka will free(3) \p payload
                                            * when it is done with it. */
        COPY = 0x2, /**< the \p payload data will be copied
                                           * and the \p payload pointer will not
                                           * be used by rdkafka after the
                                           * call returns. */
        BLOCK = 0x4, /**< Block produce*() on message queue
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
   *    RK_MSG_BLOCK - block \p produce*() call if
   *                   \p queue.buffering.max.messages or
   *                   \p queue.buffering.max.kbytes are exceeded.
   *                   Messages are considered in-queue from the point they
   *                   are accepted by produce() until their corresponding
   *                   delivery report callback/event returns.
   *                   It is thus a requirement to call 
   *                   poll() (or equiv.) from a separate
   *                   thread when RK_MSG_BLOCK is used.
   *                   See WARNING on \c RK_MSG_BLOCK above.
   *    RK_MSG_FREE - rdkafka will free(3) \p payload when it is done with it.
   *    RK_MSG_COPY - the \p payload data will be copied and the \p payload
   *               pointer will not be used by rdkafka after the
   *               call returns.
   *
   *  NOTE: RK_MSG_FREE and RK_MSG_COPY are mutually exclusive.
   *
   *  If the function returns -1 and RK_MSG_FREE was specified, then
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
        const(void)[] key = null, int msgflags = Msg.COPY, void* msg_opaque = null)
    {
        if (rd_kafka_produce(topic.rkt_, partition, msgflags, payload.ptr,
                payload.length, key.ptr, key.length, msg_opaque) == -1)
            return cast(ErrorCode) rd_kafka_errno2err(errno);
        return ErrorCode.NO_ERROR;
    }

    /**
   * Wait until all outstanding produce requests, et.al, are completed.
   *        This should typically be done prior to destroying a producer instance
   *        to make sure all queued and in-flight produce requests are completed
   *        before terminating.
   *
   * Note: This function will call poll() and thus trigger callbacks.
   *
   * TIMED_OUT if \p timeout_ms was reached before all
   *          outstanding requests were completed, else ERR_NO_ERROR
   */
    ErrorCode flush(int timeout_ms)
    {
        return cast(ErrorCode) rd_kafka_flush(rk_, timeout_ms);
    }
}

/**
 * Metadata: Broker information
 */
struct BrokerMetadata
{
    private const(rd_kafka_metadata_broker_t)* broker_metadata_;

const @property nothrow @nogc:

    /** Broker id */
    int id() const
    {
        return broker_metadata_.id;
    }

    /** Broker hostname */
    const(char)[] host() const
    {
        return broker_metadata_.host.fromStringz;
    }

    /** Broker listening port */
    int port() const
    {
        return broker_metadata_.port;
    }
}

/**
 * Metadata: Partition information
 */
struct PartitionMetadata
{
    private const (rd_kafka_metadata_partition_t)* partition_metadata_;

const @property nothrow @nogc:

    /** Partition id */
    int id()
    {
        return partition_metadata_.id;
    }

    /** Partition error reported by broker */
    ErrorCode err()
    {
        return cast(ErrorCode) partition_metadata_.err;
    }

    /** Leader broker (id) for partition */
    int leader()
    {
        return partition_metadata_.leader;
    }

    /** Replica brokers */
    auto replicas()
    {
        static struct Replicas
        {
        nothrow @nogc:
            private const(rd_kafka_metadata_partition_t)* partition_metadata_;
            private size_t _i;
            auto empty() @property
            {
                return _i >= partition_metadata_.replica_cnt;
            }

            auto front() @property
            {
                return partition_metadata_.replicas[_i];
            }

            auto popFront()
            {
                _i++;
            }

            auto length() @property
            {
                return partition_metadata_.replica_cnt - _i;
            }
        }

        return Replicas(partition_metadata_);
    }

    /** In-Sync-Replica brokers
   *  Warning: The broker may return a cached/outdated list of ISRs.
   */
    auto isrs()
    {
        static struct Isrs
        {
        nothrow @nogc:
            private const(rd_kafka_metadata_partition_t)* partition_metadata_;
            private size_t _i;
            auto empty() @property
            {
                return _i >= partition_metadata_.isr_cnt;
            }

            auto front() @property
            {
                return partition_metadata_.isrs[_i];
            }

            auto popFront()
            {
                _i++;
            }

            auto length() @property
            {
                return partition_metadata_.isr_cnt - _i;
            }
        }

        return Isrs(partition_metadata_);
    }
}

/**
 * Metadata: Topic information
 */
struct TopicMetadata
{
    private const(rd_kafka_metadata_topic_t)* topic_metadata_;

const @property nothrow @nogc:

    /** Topic name */
    const(char)[] topic()
    {
        return topic_metadata_.topic.fromStringz;
    }

    /** Partition list */
    auto partitions()
    {
        static struct Partitions
        {
        nothrow @nogc:
            private const(rd_kafka_metadata_topic_t)* topic_metadata_;
            private size_t _i;
            auto empty() @property
            {
                return _i >= topic_metadata_.partition_cnt;
            }

            auto front() @property
            {
                return PartitionMetadata(&topic_metadata_.partitions[_i]);
            }

            auto popFront()
            {
                _i++;
            }

            auto length() @property
            {
                return topic_metadata_.partition_cnt - _i;
            }
        }

        return Partitions(topic_metadata_);
    }

    /** Topic error reported by broker */
    ErrorCode err()
    {
        return cast(ErrorCode)(topic_metadata_.err);
    }
}

/**
 * Metadata container
 */
final class Metadata
{
    private const(rd_kafka_metadata_t)* metadata_;

nothrow @nogc:

    this(const(rd_kafka_metadata_t)* metadata)
    {
        metadata_ = metadata;
    }

    ~this()
    {
        rd_kafka_metadata_destroy(metadata_);
    }

const @property:

    /** Broker list */
    auto brokers() 
    {
        static struct Brokers
        {
        nothrow @nogc:
            private const(rd_kafka_metadata_t)* metadata_;
            private size_t _i;
            auto empty() @property
            {
                return _i >= metadata_.broker_cnt;
            }

            auto front() @property
            {
                return BrokerMetadata(&metadata_.brokers[_i]);
            }

            auto popFront()
            {
                _i++;
            }

            auto length() @property
            {
                return metadata_.broker_cnt - _i;
            }
        }
        assert(metadata_);
        return Brokers(metadata_);
    }

    /** Topic list */
    auto topics()
    {
        static struct Topics
        {
        nothrow @nogc:
            private const(rd_kafka_metadata_t)* metadata_;
            private size_t _i;
            auto empty() @property
            {
                return _i >= metadata_.topic_cnt;
            }

            auto front() @property
            {
                return TopicMetadata(&metadata_.topics[_i]);
            }

            auto popFront()
            {
                _i++;
            }

            auto length() @property
            {
                return metadata_.topic_cnt - _i;
            }
        }
        assert(metadata_);
        return Topics(metadata_);
    }

    /** Broker (id) originating this metadata */
    int origBrokerId()
    {
        return metadata_.orig_broker_id;
    }

    /** Broker (name) originating this metadata */
    const(char)[] origBrokerName()
    {
        return metadata_.orig_broker_name.fromStringz;
    }
}
