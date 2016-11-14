import std.stdio;
import std.exception;
import std.algorithm;
import rdkafkad;
import core.thread;
import core.time;

__gshared continuePoll = true;

void main()
{
    /// @@@@@ Configuration
    // pointer to the conf should be preserved for because delegates are used (for closures).
	auto conf = new GlobalConf;
    auto topicConf = new TopicConf;
    Producer producer;
    try
    {
        conf["metadata.broker.list"] = "localhost";
        conf["group.id"] = "rdkafkad";
        producer = new Producer(conf);
    }
    catch(Exception e)
    {
        stderr.writeln(e.msg);
        return;
    }
    conf.defaultTopicConf = topicConf;
    auto topics = [
        producer.newTopic("httplog_topic"),
    ];
    /// @@@@@ Main loop :-)
    auto handler = new Thread({ while(continuePoll) producer.poll(10);}).start;
    for (size_t c;;) foreach(topic; topics)
    {
        import std.format;
        string key = "myKey";
        string payload = format("myValue %d", c++); // use large payload for I/O benchmarks
        if(auto error = producer.produce(topic, Topic.unassignedPartition, cast(void[])payload, cast(const(void)[])key))
        {
            if(error == ErrorCode.queue_full)
            {
                writeln(error.err2str);
                Thread.sleep(10.msecs);
                continue;
            }
            stderr.writeln(error.err2str);
            continuePoll = false;
            thread_joinAll;
            return;
        }
    }
}
