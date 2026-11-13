import os
import sys

from setuptools import Extension, setup

ext_modules = []
if sys.platform == "linux":
    # The engine is io_uring and epoll. Elsewhere the package is pure Python
    # and hands out asyncio's loop (see ringloop/__init__.py).
    from Cython.Build import cythonize

    debug = os.environ.get("RINGLOOP_DEBUG_BUILD") == "1"
    ext = Extension(
        "ringloop._core",
        ["src/ringloop/_core.pyx"],
        include_dirs=["src/ringloop"],
        depends=["src/ringloop/ll_uring.h", "src/ringloop/ll_abi.h", "src/ringloop/ll_sync.h"],  # rebuild when it changes; ship it in the sdist
        extra_compile_args=["-O0", "-g3"] if debug else ["-O2", "-g0"],
    )
    ext_modules = cythonize([ext], compiler_directives={"language_level": "3"})

setup(ext_modules=ext_modules)
