///
module rdkafkad.metadata;
import rdkafkad;
import rdkafkad.iodriver;

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
