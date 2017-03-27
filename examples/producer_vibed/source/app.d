import std.stdio;
import std.exception;
import std.algorithm;
import std.file;

import rdkafkad;
import vibe.d;

__gshared continuePoll = true;

void main()
{
    /// @@@@@ Configuration
    // pointer to the conf should be preserved for because delegates are used (for closures).
	auto conf = new GlobalConf;
    Producer producer;
    try
    {
        conf.fromText("global.conf".readText);
        producer = new Producer(conf);
    }
    catch(Exception e)
    {
        stderr.writeln(e.msg);
        return;
    }
    auto topics = [
        producer.newTopic("httplog_topic"),
    ];

    auto condition = new TaskCondition(new TaskMutex);
    /// @@@@@ Main loop :-)
    runTask({
        while(continuePoll)
        {
            producer.poll();
        }
        condition.notify;
        });
    for (size_t c;;) foreach(topic; topics)
    {
        import std.format;
        string key = "myKey";
        string payload = format("myValue %d", c++); // use large payload for I/O benchmarks
        sleep(100.msecs); // comment for I/O benchmarks
        if(auto error = producer.produce(topic, Topic.unassignedPartition, cast(void[])payload, cast(const(void)[])key))
        {
            if(error == ErrorCode.queue_full)
            {
                writeln(error.err2str);
                sleep(10.msecs);
                continue;
            }
            stderr.writeln(error.err2str);
            continuePoll = false;
            synchronized(condition.mutex)
                condition.wait;
            return;
        }
    }
}
