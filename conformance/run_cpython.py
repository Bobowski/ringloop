"""Run CPython's own asyncio test suite on ringloop.

    python conformance/run_cpython.py --lib /path/to/cpython/Lib [-v] [module ...]

`--lib` is the `Lib` directory of a CPython source tree of the running
version (the `test` package is not part of most installations). It is
appended to `sys.path`, so only `test` comes from it.

Every way the suite makes a loop gives a `ringloop.Loop`:
`asyncio.new_event_loop()`, `asyncio.run()`, `asyncio.EventLoop` and
`asyncio.SelectorEventLoop`. Tests that examine the internals of asyncio's
own loop classes (selector mocks, private attributes) cannot apply to another
loop; `conformance/cpython_skips.txt` lists each with its reason.

A line of that file is `pattern: reason`; the pattern is matched (fnmatch)
against `module.Class.test_name`. `[3.12,3.13] pattern: reason` applies to
those CPython versions only, `[3.14+] pattern: reason` to 3.14 and newer.

The exit status is 0 when every test that is not listed passed and (in a
full run) every entry of the list matched a test.
"""

import argparse
import asyncio
import asyncio.unix_events
import fnmatch
import os
import signal
import sys
import unittest

TEST_TIMEOUT = 120

MODULES = [
    "test_buffered_proto", "test_context", "test_eager_task_factory",
    "test_events", "test_free_threading", "test_futures", "test_futures2",
    "test_graph", "test_locks", "test_pep492", "test_protocols",
    "test_queues", "test_runners", "test_sendfile", "test_server",
    "test_server_context", "test_sock_lowlevel", "test_ssl", "test_sslproto",
    "test_staggered", "test_streams", "test_subprocess", "test_taskgroups",
    "test_tasks", "test_threads", "test_timeouts", "test_tools",
    "test_transports", "test_waitfor",
]


def _version_matches(spec):
    major, minor = spec.rstrip("+").split(".")
    wanted = (int(major), int(minor))
    return sys.version_info[:2] >= wanted if spec.endswith("+") else sys.version_info[:2] == wanted


def read_skips(path):
    skips = {}
    with open(path) as f:
        for line in f:
            line = line.split("#", 1)[0].strip()
            if not line:
                continue
            if line.startswith("["):
                # "[3.12,3.13] pattern: reason": those versions only;
                # "[3.14+]": that version and newer.
                versions, _, line = line[1:].partition("]")
                if not any(_version_matches(v.strip()) for v in versions.split(",")):
                    continue
            pattern, _, reason = line.partition(":")
            skips[pattern.strip()] = reason.strip()
    return skips


def apply_skips(suite, skips, used):
    for test in suite:
        if isinstance(test, unittest.TestSuite):
            apply_skips(test, skips, used)
            continue
        ident = test.id().removeprefix("test.test_asyncio.")
        for pattern, reason in skips.items():
            if fnmatch.fnmatchcase(ident, pattern):
                used.add(pattern)
                method = getattr(test, test._testMethodName)
                setattr(test, test._testMethodName,
                        unittest.skip("ringloop: " + reason)(method))
                break


class TestTimeout(Exception):
    pass


class Result(unittest.TextTestResult):
    """A test that hangs fails after TEST_TIMEOUT seconds; the rest still run."""

    def startTest(self, test):
        super().startTest(test)
        signal.alarm(TEST_TIMEOUT)

    def stopTest(self, test):
        signal.alarm(0)
        super().stopTest(test)


def _on_alarm(signum, frame):
    raise TestTimeout(f"no result after {TEST_TIMEOUT} seconds")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lib", required=True)
    ap.add_argument("-v", action="store_true")
    ap.add_argument("-f", "--failfast", action="store_true")
    ap.add_argument("-k", action="append", default=[])
    ap.add_argument("modules", nargs="*")
    args = ap.parse_args()

    sys.path.append(args.lib)
    import ringloop

    class Loop(ringloop.Loop):
        def __init__(self, selector=None):  # the suite passes selectors
            super().__init__()

    asyncio.unix_events._UnixDefaultEventLoopPolicy._loop_factory = Loop
    asyncio.SelectorEventLoop = Loop
    asyncio.EventLoop = Loop

    here = os.path.dirname(os.path.abspath(__file__))
    skips = read_skips(os.path.join(here, "cpython_skips.txt"))
    used = set()
    loader = unittest.TestLoader()
    if args.k:
        loader.testNamePatterns = [k if "*" in k else f"*{k}*" for k in args.k]
    suite = unittest.TestSuite()
    import importlib.util
    for name in args.modules or MODULES:
        if importlib.util.find_spec(f"test.test_asyncio.{name}") is None:
            print(f"{name}: not in this CPython version")
            continue
        suite.addTests(loader.loadTestsFromName(f"test.test_asyncio.{name}"))
    apply_skips(suite, skips, used)
    probe = ringloop.new_event_loop()
    print(f"ringloop backend: {probe.backend}", flush=True)
    probe.close()
    signal.signal(signal.SIGALRM, _on_alarm)
    result = unittest.TextTestRunner(
        verbosity=2 if args.v else 1, failfast=args.failfast, resultclass=Result).run(suite)
    if not args.modules and not args.k:
        for key in sorted(set(skips) - used):
            print(f"stale entry in cpython_skips.txt: {key}")
        if set(skips) - used:
            sys.exit(1)
    sys.exit(0 if result.wasSuccessful() else 1)


if __name__ == "__main__":
    main()
