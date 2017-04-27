import std.file;

import vibe.d;
import vibe.stream.stdio;
import rdkafkad;
import std.datetime: SysTime;

void hello(HTTPServerRequest req, HTTPServerResponse res)
{
    res.writeBody("Hello, World!");
}

void main()
{
    auto stdout = new StdoutStream;
    /// @@@@@ Configuration
    // pointer to the conf should be preserved for because delegates are used (for closures).
    auto tconf = new TopicConf;
    tconf["auto.offset.reset"] = "earliest";
	auto conf = new GlobalConf(tconf);
    KafkaConsumer consumer;
    try
    {
        conf.fromText("global.conf".readText);
        consumer = new KafkaConsumer(conf);
    }
    catch(Exception e)
    {
        import std.stdio;
        stderr.writeln(e.msg);
        return;
    }
    consumer.subscribe("bigdata", /+...+/);
    import vibe.stream.taskpipe;
    auto stream = new TaskPipe;
    ubyte[64*1024] buffer = void;
    runTask({
            import std.stdio;
        auto fout = File("temp2", "w");
        while (!stream.empty) {
            import std.algorithm.comparison;
            size_t chunk = min(stream.leastSize, buffer.length);
            assert(chunk > 0, "leastSize returned zero for non-empty stream.");
            //logTrace("read pipe chunk %d", chunk);
            stream.read(buffer[0 .. chunk]);

            fout.writeln(cast(string) buffer[0 .. chunk]);
        }
    });
    runTask({
        for (size_t c;;)
        {
            Message msg;
            consumer.consume(msg, 0);
            if(auto error = msg.err)
            {
                if (msg.err == ErrorCode.timed_out || msg.err == ErrorCode.msg_timed_out)
                {
                    sleep(1.msecs);
                }
                else if (msg.err == ErrorCode.partition_eof)
                {
                    exitEventLoop;
                    break;
                }
                else
                {
                    logError("Error: ", error.err2str);
                    vibe.core.core.yield;
                }
                continue;
            }
            stream.write(cast(const(ubyte[])) msg.payload, IOMode.all); /// vibe streams :-)
            stream.write(cast(const(ubyte[])) "\n", IOMode.all);
        }
        stream.finalize;
    });

    auto settings = new HTTPServerSettings;
    settings.port = 8080;
    settings.bindAddresses = ["::1", "127.0.0.1"];
    listenHTTP(settings, &hello);

    runEventLoop;
}
