# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import sys
import warnings

import ez_setup
ez_setup.use_setuptools()

from setuptools import setup
from distutils.command.build_ext import build_ext
from distutils.core import Extension
from distutils.errors import (CCompilerError, DistutilsPlatformError,
                              DistutilsExecError)
from distutils.cmd import Command

try:
    import subprocess
    has_subprocess = True
except ImportError:
    has_subprocess = False

from cassandra import __version__

long_description = ""
with open("README.rst") as f:
    long_description = f.read()

has_cqlengine = False
if __name__ == '__main__' and sys.argv[1] == "install":
    try:
        import cqlengine
        has_cqlengine = True
    except ImportError:
        pass

PROFILING = False


class DocCommand(Command):

    description = "generate or test documentation"

    user_options = [("test", "t",
                     "run doctests instead of generating documentation")]

    boolean_options = ["test"]

    def initialize_options(self):
        self.test = False

    def finalize_options(self):
        pass

    def run(self):
        if self.test:
            path = "docs/_build/doctest"
            mode = "doctest"
        else:
            path = "docs/_build/%s" % __version__
            mode = "html"

            try:
                os.makedirs(path)
            except OSError:
                pass

        if has_subprocess:
            # Prevent run with in-place extensions because cython-generated objects do not carry docstrings
            # http://docs.cython.org/src/userguide/special_methods.html#docstrings
            import glob
            for f in glob.glob("cassandra/*.so"):
                print("Removing '%s' to allow docs to run on pure python modules." % (f,))
                os.unlink(f)

            # Build io extension to make import and docstrings work
            try:
                output = subprocess.check_output(
                    ["python", "setup.py", "build_ext", "--inplace", "--force", "--no-murmur3", "--no-cython"],
                    stderr=subprocess.STDOUT)
            except subprocess.CalledProcessError as exc:
                raise RuntimeError("Documentation step '%s' failed: %s: %s" % ("build_ext", exc, exc.output))
            else:
                print(output)

            try:
                output = subprocess.check_output(
                    ["sphinx-build", "-b", mode, "docs", path],
                    stderr=subprocess.STDOUT)
            except subprocess.CalledProcessError as exc:
                raise RuntimeError("Documentation step '%s' failed: %s: %s" % (mode, exc, exc.output))
            else:
                print(output)

            print("")
            print("Documentation step '%s' performed, results here:" % mode)
            print("   file://%s/%s/index.html" % (os.path.dirname(os.path.realpath(__file__)), path))


class BuildFailed(Exception):

    def __init__(self, ext):
        self.ext = ext


is_windows = sys.platform.startswith('win32')
is_macos = sys.platform.startswith('darwin')

murmur3_ext = Extension('cassandra.cmurmur3',
                        sources=['cassandra/cmurmur3.c'])


def eval_env_var_as_array(varname):
    val = os.environ.get(varname)
    return None if not val else [v.strip() for v in val.split(',')]


DEFAULT_LIBEV_INCLUDES = ['/usr/include/libev', '/usr/local/include', '/opt/local/include', '/usr/include']
DEFAULT_LIBEV_LIBDIRS = ['/usr/local/lib', '/opt/local/lib', '/usr/lib64']
libev_includes = eval_env_var_as_array('CASS_DRIVER_LIBEV_INCLUDES') or DEFAULT_LIBEV_INCLUDES
libev_libdirs = eval_env_var_as_array('CASS_DRIVER_LIBEV_LIBS') or DEFAULT_LIBEV_LIBDIRS
if is_macos:
    libev_includes.extend(['/opt/homebrew/include', os.path.expanduser('~/homebrew/include')])
    libev_libdirs.extend(['/opt/homebrew/lib'])
libev_ext = Extension('cassandra.io.libevwrapper',
                      sources=['cassandra/io/libevwrapper.c'],
                      include_dirs=libev_includes,
                      libraries=['ev'],
                      library_dirs=libev_libdirs)

platform_unsupported_msg = \
"""
===============================================================================
The optional C extensions are not supported on this platform.
===============================================================================
"""

arch_unsupported_msg = \
"""
===============================================================================
The optional C extensions are not supported on big-endian systems.
===============================================================================
"""

pypy_unsupported_msg = \
"""
=================================================================================
Some optional C extensions are not supported in PyPy. Only murmur3 will be built.
=================================================================================
"""

is_pypy = "PyPy" in sys.version
if is_pypy:
    sys.stderr.write(pypy_unsupported_msg)

is_supported_platform = sys.platform != "cli" and not sys.platform.startswith("java")
is_supported_arch = sys.byteorder != "big"
if not is_supported_platform:
    sys.stderr.write(platform_unsupported_msg)
elif not is_supported_arch:
    sys.stderr.write(arch_unsupported_msg)

try_extensions = "--no-extensions" not in sys.argv and is_supported_platform and is_supported_arch and not os.environ.get('CASS_DRIVER_NO_EXTENSIONS')
try_murmur3 = try_extensions and "--no-murmur3" not in sys.argv
try_libev = try_extensions and "--no-libev" not in sys.argv and not is_pypy and not os.environ.get('CASS_DRIVER_NO_LIBEV')
try_cython = try_extensions and "--no-cython" not in sys.argv and not is_pypy and not os.environ.get('CASS_DRIVER_NO_CYTHON')
try_cython &= 'egg_info' not in sys.argv  # bypass setup_requires for pip egg_info calls, which will never have --install-option"--no-cython" coming fomr pip

sys.argv = [a for a in sys.argv if a not in ("--no-murmur3", "--no-libev", "--no-cython", "--no-extensions")]

build_concurrency = int(os.environ.get('CASS_DRIVER_BUILD_CONCURRENCY', '0'))


class NoPatchExtension(Extension):

    # Older versions of setuptools.extension has a static flag which is set False before our
    # setup_requires lands Cython. It causes our *.pyx sources to be renamed to *.c in
    # the initializer.
    # The other workaround would be to manually generate sources, but that bypasses a lot
    # of the niceness cythonize embodies (setup build dir, conditional build, etc).
    # Newer setuptools does not have this problem because it checks for cython dynamically.
    # https://bitbucket.org/pypa/setuptools/commits/714c3144e08fd01a9f61d1c88411e76d2538b2e4

    def __init__(self, *args, **kwargs):
        # bypass the patched init if possible
        if Extension.__bases__:
            base, = Extension.__bases__
            base.__init__(self, *args, **kwargs)
        else:
            Extension.__init__(self, *args, **kwargs)


class build_extensions(build_ext):

    error_message = """
===============================================================================
WARNING: could not compile %s.

The C extensions are not required for the driver to run, but they add support
for token-aware routing with the Murmur3Partitioner.

On Windows, make sure Visual Studio or an SDK is installed, and your environment
is configured to build for the appropriate architecture (matching your Python runtime).
This is often a matter of using vcvarsall.bat from your install directory, or running
from a command prompt in the Visual Studio Tools Start Menu.
===============================================================================
""" if is_windows else """
===============================================================================
WARNING: could not compile %s.

The C extensions are not required for the driver to run, but they add support
for libev and token-aware routing with the Murmur3Partitioner.

Linux users should ensure that GCC and the Python headers are available.

On Ubuntu and Debian, this can be accomplished by running:

    $ sudo apt-get install build-essential python-dev

On RedHat and RedHat-based systems like CentOS and Fedora:

    $ sudo yum install gcc python-devel

On OSX, homebrew installations of Python should provide the necessary headers.

libev Support
-------------
For libev support, you will also need to install libev and its headers.

On Debian/Ubuntu:

    $ sudo apt-get install libev4 libev-dev

On RHEL/CentOS/Fedora:

    $ sudo yum install libev libev-devel

On OSX, via homebrew:

    $ brew install libev

===============================================================================
    """

    def run(self):
        try:
            self._setup_extensions()
            build_ext.run(self)
        except DistutilsPlatformError as exc:
            sys.stderr.write('%s\n' % str(exc))
            warnings.warn(self.error_message % "C extensions.")

    def build_extensions(self):
        if build_concurrency > 1:
            self.check_extensions_list(self.extensions)

            import multiprocessing.pool
            multiprocessing.pool.ThreadPool(processes=build_concurrency).map(self.build_extension, self.extensions)
        else:
            build_ext.build_extensions(self)

    def build_extension(self, ext):
        try:
            build_ext.build_extension(self, ext)
        except (CCompilerError, DistutilsExecError,
                DistutilsPlatformError, IOError) as exc:
            sys.stderr.write('%s\n' % str(exc))
            name = "The %s extension" % (ext.name,)
            warnings.warn(self.error_message % (name,))

    def _setup_extensions(self):
        # We defer extension setup until this command to leveraage 'setup_requires' pulling in Cython before we
        # attempt to import anything
        self.extensions = []

        if try_murmur3:
            self.extensions.append(murmur3_ext)

        if try_libev:
            sys.stderr.write("Appending libev extension %s" % libev_ext)
            self.extensions.append(libev_ext)

        if try_cython:
            try:
                from Cython.Build import cythonize
                cython_candidates = ['cluster', 'concurrent', 'connection', 'cqltypes', 'metadata',
                                     'pool', 'protocol', 'query', 'util']
                compile_args = [] if is_windows else ['-Wno-unused-function']
                self.extensions.extend(cythonize(
                    [Extension('cassandra.%s' % m, ['cassandra/%s.py' % m],
                               extra_compile_args=compile_args)
                        for m in cython_candidates],
                    nthreads=build_concurrency,
                    exclude_failures=True))

                self.extensions.extend(cythonize(NoPatchExtension("*", ["cassandra/*.pyx"], extra_compile_args=compile_args),
                                                 nthreads=build_concurrency))
            except Exception:
                sys.stderr.write("Failed to cythonize one or more modules. These will not be compiled as extensions (optional).\n")


def pre_build_check():
    """
    Try to verify build tools
    """
    if os.environ.get('CASS_DRIVER_NO_PRE_BUILD_CHECK'):
        return True

    try:
        from distutils.ccompiler import new_compiler
        from distutils.sysconfig import customize_compiler
        from distutils.dist import Distribution

        # base build_ext just to emulate compiler option setup
        be = build_ext(Distribution())
        be.initialize_options()
        be.finalize_options()

        # First, make sure we have a Python include directory
        have_python_include = any(os.path.isfile(os.path.join(p, 'Python.h')) for p in be.include_dirs)
        if not have_python_include:
            sys.stderr.write("Did not find 'Python.h' in %s.\n" % (be.include_dirs,))
            return False

        compiler = new_compiler(compiler=be.compiler)
        customize_compiler(compiler)

        try:
            # We must be able to initialize the compiler if it has that method
            if hasattr(compiler, "initialize"):
                compiler.initialize()
        except OSError:
            return False

        executables = []
        if compiler.compiler_type in ('unix', 'cygwin'):
            executables = [compiler.executables[exe][0] for exe in ('compiler_so', 'linker_so')]
        elif compiler.compiler_type == 'nt':
            executables = [getattr(compiler, exe) for exe in ('cc', 'linker')]

        if executables:
            from distutils.spawn import find_executable
            for exe in executables:
                if not find_executable(exe):
                    sys.stderr.write("Failed to find %s for compiler type %s.\n" % (exe, compiler.compiler_type))
                    return False

    except Exception as exc:
        sys.stderr.write('%s\n' % str(exc))
        sys.stderr.write("Failed pre-build check. Attempting anyway.\n")

    # if we are unable to positively id the compiler type, or one of these assumptions fails,
    # just proceed as we would have without the check
    return True


def run_setup(extensions):

    kw = {'cmdclass': {'doc': DocCommand}}
    kw['cmdclass']['build_ext'] = build_extensions
    kw['ext_modules'] = [Extension('DUMMY', [])]  # dummy extension makes sure build_ext is called for install

    if try_cython:
        # precheck compiler before adding to setup_requires
        # we don't actually negate try_cython because:
        # 1.) build_ext eats errors at compile time, letting the install complete while producing useful feedback
        # 2.) there could be a case where the python environment has cython installed but the system doesn't have build tools
        if pre_build_check():
            cython_dep = 'Cython>=3.0'
            user_specified_cython_version = os.environ.get('CASS_DRIVER_ALLOWED_CYTHON_VERSION')
            if user_specified_cython_version is not None:
                cython_dep = 'Cython==%s' % (user_specified_cython_version,)
            kw['setup_requires'] = [cython_dep]
        else:
            sys.stderr.write("Bypassing Cython setup requirement\n")

    dependencies = ['geomet>=1.1']

    _EXTRAS_REQUIRE = {
        'graph': ['gremlinpython==3.4.6'],
        'cle': ['cryptography>=42.0']
    }

    setup(
        name='cassandra-driver',
        version=__version__,
        description='Apache Cassandra Python Driver',
        long_description=long_description,
        long_description_content_type='text/x-rst',
        url='http://github.com/datastax/python-driver',
        project_urls={
            'Documentation': 'https://docs.datastax.com/en/developer/python-driver/latest/',
            'Source': 'https://github.com/datastax/python-driver/',
            'Issues': 'https://datastax-oss.atlassian.net/browse/PYTHON',
        },
        author='DataStax',
        packages=[
            'cassandra', 'cassandra.io', 'cassandra.cqlengine', 'cassandra.graph',
            'cassandra.datastax', 'cassandra.datastax.insights', 'cassandra.datastax.graph',
            'cassandra.datastax.graph.fluent', 'cassandra.datastax.cloud',
            "cassandra.column_encryption"
        ],
        keywords='cassandra,cql,orm,dse,graph',
        include_package_data=True,
        install_requires=dependencies,
        extras_require=_EXTRAS_REQUIRE,
        tests_require=['pytest', 'PyYAML', 'pytz'],
        classifiers=[
            'Development Status :: 5 - Production/Stable',
            'Intended Audience :: Developers',
            'License :: OSI Approved :: Apache Software License',
            'Natural Language :: English',
            'Operating System :: OS Independent',
            'Programming Language :: Python',
            'Programming Language :: Python :: 3.9',
            'Programming Language :: Python :: 3.10',
            'Programming Language :: Python :: 3.11',
            'Programming Language :: Python :: 3.12',
            'Programming Language :: Python :: 3.13',
            'Programming Language :: Python :: Implementation :: CPython',
            'Programming Language :: Python :: Implementation :: PyPy',
            'Topic :: Software Development :: Libraries :: Python Modules'
        ],
        **kw)


run_setup(None)

if has_cqlengine:
    warnings.warn("\n#######\n'cqlengine' package is present on path: %s\n"
                  "cqlengine is now an integrated sub-package of this driver.\n"
                  "It is recommended to remove this package to reduce the chance for conflicting usage" % cqlengine.__file__)
