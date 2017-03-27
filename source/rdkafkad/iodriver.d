module rdkafkad.iodriver;

version(Have_vibe_d)
{
    enum have_vibed = true;
    import vibe.core.task: Task;
    import vibe.core.concurrency: receiveCompat;
    import vibe.core.core: runWorkerTaskH;
    public import vibe.core.concurrency: yield, sendCompat;
    alias _IODelegate = immutable void delegate() @nogc nothrow;

    // workaround
    private auto receiveOnlyCompat(ARG)()
    {
        import std.meta: Unqual;
        import std.concurrency: LinkTerminated, OwnerTerminated, MessageMismatch;
        import std.variant: Variant;
        import std.format: format;
        Unqual!ARG ret;

        receiveCompat(
            (ARG val) { ret = val; },
            (LinkTerminated e) { throw e; },
            (OwnerTerminated e) { throw e; },
            (Variant val) { throw new MessageMismatch(format("Unexpected message type %s, expected %s.", val.type, ARG.stringof)); }
        );

        return cast(ARG)ret;
    }

    __gshared Task _io_task;
    enum IO(string code) = "
    {
        bool done;
        _IODelegate _io_delegate_ = ()
            {
                " ~ code ~ "
                done = true;
            };
        sendCompat(_io_task, _io_delegate_);
        while(!done) yield;
    }";

    shared static this()
    {
        _io_task = runWorkerTaskH(&_io_handler);
    }

    void _io_handler()
    {
        for(;;)
        {
            auto call = receiveOnlyCompat!_IODelegate;
            call();
        }
    }
}
else
version(Have_vibe_core)
{
    enum have_vibed = true;
    static assert("rdkafkad: support for vibe-core >=1.0.0 is not implemented yet.");
}
else
{
    enum have_vibed = false;
    enum IO(string code) = "{" ~ code ~ "}";
}
