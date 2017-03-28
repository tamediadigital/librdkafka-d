module rdkafkad.iodriver;
public import rdkafkad.iodriver0;

version(Have_vibe_d)
{
    shared static this()
    {
        _io_task = runWorkerTaskH(&_io_handler);
    }
}
else
version(Have_vibe_core)
{
    static assert("rdkafkad: support for vibe-core >=1.0.0 is not implemented yet.");
}
else
{
}
