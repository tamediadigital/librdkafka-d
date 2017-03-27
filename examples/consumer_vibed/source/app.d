import std.stdio;
import std.file;

import vibe.d;
import rdkafkad;

void main()
{
    /// @@@@@ Configuration
    // pointer to the conf should be preserved for because delegates are used (for closures).
	auto conf = new GlobalConf;
    KafkaConsumer consumer;
    try
    {
        conf.fromText("global.conf".readText);
        consumer = new KafkaConsumer(conf);
    }
    catch(Exception e)
    {
        stderr.writeln(e.msg);
        return;
    }
    consumer.subscribe("httplog_topic", /+...+/);
    for (size_t c;;)
    {
        if(++c % 100 == 0) // use large number for fast I/O!
            consumer.commitSync();
        Message msg;
        consumer.consume(msg, 6000);
        if(auto error = msg.err)
        {
            writeln("Error: ", error.err2str);
            continue;
        }
        auto ts = msg.timestamp;
        with(msg) 
            writefln("Topic %s[%d]->%d, key: %s, timestamp(%s) = %s",
                    topicName, partition, offset, cast(const(char)[])key,
                    ts.type, ts.timestamp.fromUnixTime);
    }
}
