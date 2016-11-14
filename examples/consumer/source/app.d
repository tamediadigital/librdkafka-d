import std.stdio;
import rdkafkad;

void main()
{
    /// @@@@@ Configuration
    // pointer to the conf should be preserved for because delegates are used (for closures).
	auto conf = new GlobalConf;
    auto topicConf = new TopicConf;
    KafkaConsumer consumer;
    try
    {
        conf["metadata.broker.list"] = "localhost";
        conf["group.id"] = "rdkafkad";
        consumer = new KafkaConsumer(conf);
    }
    catch(Exception e)
    {
        stderr.writeln(e.msg);
        return;
    }
    conf.defaultTopicConf = topicConf;
    consumer.subscribe("httplog_topic", /+...+/);
    for (size_t c;;)
    {
        if(++c % 100 == 0) // use large number for fast I/O!
            consumer.commitSync();
        Message msg;
        consumer.consume(6000, msg);
        if(auto error = msg.err)
        {
            writeln("Error: ", error.err2str);
            continue;
        }
        with(msg) writefln("Topic %s[%d]->%d, key: %s",
                    topicName, partition, offset, cast(const(char)[])key);
    }
}
