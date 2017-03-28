module rdkafkad.iodriver0;

version(Have_vibe_d)
{
    enum bool have_vibed = true;
    public import vibe.core.task: Task;
    public import vibe.core.concurrency: receiveCompat, Isolated;
    public import vibe.core.core: runWorkerTaskH;
    alias _IODelegate = immutable void delegate();

    // workaround
    private auto receiveOnlyCompat(ARG)()
    {
        import std.meta: Unqual;
        import std.concurrency: LinkTerminated, OwnerTerminated, MessageMismatch;
        import std.variant: Variant;
        import std.format: format;
        Unqual!ARG ret;

        receiveCompat(
            (Isolated!ARG val) { ret = val.extract; },
            (LinkTerminated e) { throw e; },
            (OwnerTerminated e) { throw e; },
            (Variant val) { throw new MessageMismatch(format("Unexpected message type %s, expected %s.", val.type, ARG.stringof)); }
        );

        return cast(ARG)ret;
    }

    class _IODelegateClass
    {
        _IODelegate call;

        this(_IODelegate call) nothrow @nogc pure @safe
        {
            this.call = call;
        }

        void opCall()
        {
            call();
        }
    }

    __gshared Task _io_task;
    enum IO(string code) = "
    {
        import vibe.core.sync: TaskCondition, TaskMutex;
        import vibe.core.concurrency: sendCompat, assumeIsolated;
        import core.time : msecs;
        auto condition = new TaskCondition(new TaskMutex);
        _IODelegate _io_delegate_ = ()
            {
                " ~ code ~ "
                condition.notify;
            };
        sendCompat(_io_task, assumeIsolated(new _IODelegateClass(_io_delegate_)));
        condition.mutex.lock;
        condition.wait;
        condition.mutex.unlock;
    }";

    void _io_handler()
    {
        for(;;)
        {
            auto call = receiveOnlyCompat!_IODelegateClass;
            call();
        }
    }
}
else
version(Have_vibe_core)
{
    enum bool have_vibed = true;
    static assert("rdkafkad: support for vibe-core >=1.0.0 is not implemented yet.");
}
else
{
    enum bool have_vibed = false;
    enum IO(string code) = "{" ~ code ~ "}";
}