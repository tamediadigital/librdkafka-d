///
module rdkafkad.config;
import rdkafkad;
import rdkafkad.iodriver;

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
abstract class Conf
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

    ///
    string[string] dump();

    private static string[string] dumpImpl(const (char)** arrc, size_t cnt)
    {
        assert(cnt % 2 == 0);
        string[string] aa;
        foreach (size_t i; 0 .. cnt / 2)
            aa[arrc[i * 2].fromStringz.idup] = arrc[i * 2 + 1].fromStringz.idup;
        rd_kafka_conf_dump_free(arrc, cnt);
        return aa;
    }

    ///
    void fromText(Conf conf, string text)
    {
        assert(conf);
        import std.algorithm;
        import std.string;
        import std.format;
        import std.conv: ConvException;
        size_t i;
        foreach(line; text.lineSplitter)
        {
            i++;
            line = line.findSplit("#")[0].strip;
            if (line.empty)
                continue;
            auto t = line.findSplit("=");
            if (t[1].empty)
                throw new ConvException(format("failed to parse configuraiton at line %s", i));
            auto key = t[0].stripRight;
            auto value = t[2].stripLeft;
            conf[key] = value;
        }
    }
}


///
class GlobalConf : Conf
{
    ///
    this(TopicConf defaultTopicConf = new TopicConf)
    {
        rk_conf_ = rd_kafka_conf_new;
        this.defaultTopicConf = defaultTopicConf;
    }

    private TopicConf _defaultTopicConf;

    /** Dump configuration names and values to list containing
   *         name,value tuples */
    override string[string] dump()
    {
        size_t cnt;
        auto arrc = rd_kafka_conf_dump(rk_conf_, &cnt);
        return dumpImpl(arrc, cnt);
    }

    /**
     * Set configuration property \p name to value \p value.
     * OK on success, else writes a human readable error
     *          description to \p errstr on error.
     */
    override void opIndexAssign(in char[] value, in char[] name)
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
    override string opIndex(in char[] name) const
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

    package DeliveryReportCb dr_cb_;
    package EventCb event_cb_;
    package SocketCb socket_cb_;
    package OpenCb open_cb_;
    package RebalanceCb rebalance_cb_;
    package OffsetCommitCb offset_commit_cb_;
    package rd_kafka_conf_t* rk_conf_;

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
     * subscribed topics and for Topic construction with producer.
     *
     * See_also: subscribe()
     */
    void defaultTopicConf(TopicConf topic_conf) @property
    {
        _defaultTopicConf = topic_conf;
        rd_kafka_conf_set_default_topic_conf(rk_conf_,
            rd_kafka_topic_conf_dup(topic_conf.rkt_conf_));
    }

    ///ditto
    TopicConf defaultTopicConf() @property
    {
        return _defaultTopicConf;
    }

}

class TopicConf : Conf
{
    /**
     * Set configuration property \p name to value \p value.
     * OK on success, else writes a human readable error
     *          description to \p errstr on error.
     */
    override void opIndexAssign(in char[] value, in char[] name)
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
    override string opIndex(in char[] name) const
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
    override string[string] dump()
    {
        size_t cnt;
        auto arrc = rd_kafka_topic_conf_dump(rkt_conf_, &cnt);
        return dumpImpl(arrc, cnt);
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

    package PartitionerCb partitioner_cb_;
    package PartitionerKeyPointerCb partitioner_kp_cb_;
    package rd_kafka_topic_conf_t* rkt_conf_;

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
