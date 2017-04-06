import std.stdio;
import std.exception;
import std.algorithm;
import std.datetime;
import rdkafkad;
import core.thread;

__gshared continuePoll = true;

void main()
{
    /// @@@@@ Configuration
    // pointer to the conf should be preserved for because delegates are used (for closures).
	auto conf = new GlobalConf;
    Producer producer;
    try
    {
        conf["metadata.broker.list"] = "localhost";
        producer = new Producer(conf);
    }
    catch(Exception e)
    {
        stderr.writeln(e.msg);
        return;
    }
    auto topics = [
        producer.newTopic("httplog_topic3"),
    ];
    auto time = Clock.currTime(UTC());
    /// @@@@@ Main loop :-)
    auto handler = new Thread({ while(continuePoll) producer.poll(10);}).start;
    for (size_t c;;) foreach(topic; topics)
    {
        import std.format;
        string key = "myKey";
        string payload = format(`{"ts":%s, "myValue":%d}`, (time + minutes(c)).toUnixTime, c++); // use large payload for I/O benchmarks
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
