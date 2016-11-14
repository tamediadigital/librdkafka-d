import std.stdio;
import rdkafkad;
import vibe.d;

__gshared Task producerTask;
__gshared continuePoll = true;
    // pointer to the conf should be preserved for because delegates are used (for closures).
__gshared GlobalConf conf;
__gshared TopicConf topicConf;
__gshared Producer producer;

shared static this()
{
    debug setLogLevel(LogLevel.debug_);

    auto settings = new HTTPServerSettings;
    settings.port = 8080;
    
    listenHTTP(settings, &handleRequest);
    conf = new GlobalConf;
    topicConf = new TopicConf;
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
    auto topic = producer.newTopic("httplog_topic");
    /// @@@@@ Main loop
    runWorkerTask({ while(continuePoll) producer.poll(10);});
    producerTask = runTask({for (;;){
        receive((string msg) {
            if(auto error = producer.produce(topic, Topic.unassignedPartittin, cast(void[])msg))
            {
                if(error == ErrorCode.queue_full)
                {
                    logInfo(error.err2str);
                    vibe.core.core.yield;
                }
                stderr.writeln(error.err2str);
                continuePoll = false;
                exitEventLoop(true);
            }
        });
    }});

    setMaxMailboxSize(producerTask.tid, 100, OnCrowding.throwException);
}

void handleRequest(HTTPServerRequest req,
    HTTPServerResponse res)
{
    if (req.path == "/")
        res.writeBody("Hello, World!", "text/plain");

    try {
        send(producerTask, req.path);
    } catch (MailboxFull) {
        // message queue is full, this may indicate that producer is blocking, no connection, etc.
        logInfo("Couldn't log to kafka");
    }
}
