This package contains many of the internal implementations for the mock.

Some of the stuff in this package is made public in spite of not being used specifically
to enable testing from the mockimpl package to avoid circular dependancies. For instance
the configgen stuff needs a cluster from mockimpl to test with, but mockimpl depends on
the configgen itself.
