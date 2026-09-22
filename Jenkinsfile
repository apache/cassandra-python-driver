#!groovy
/*

There are multiple combinations to test the python driver.

Test Profiles:

  Full: Execute all unit and integration tests, including long tests.
  Standard: Execute unit and integration tests.
  Smoke Tests: Execute a small subset of tests.
  EVENT_LOOP: Execute a small subset of tests selected to test EVENT_LOOPs.

Matrix Types:

  Full: All server versions, python runtimes tested with and without Cython.
  Cassandra: All cassandra server versions.
  Dse: All dse server versions.
  Hcd: All hcd server versions.
  Smoke: CI-friendly configurations. Currently-supported Python version + modern Cassandra/DSE instances.
         We also avoid cython since it's tested as part of the nightlies

Parameters: 

  EVENT_LOOP: 'LIBEV' (Default), 'ASYNCIO', 'ASYNCORE'
  CYTHON: Default, 'True', 'False'

*/

@Library('dsdrivers-pipeline-lib@develop')
import com.datastax.jenkins.drivers.python.Slack

slack = new Slack()

DEFAULT_CASSANDRA = ['3.11', '4.0', '4.1', '5.0']
DEFAULT_DSE = ['dse-5.1.35', 'dse-6.8.30', 'dse-6.9.0']
DEFAULT_HCD = ['hcd-1.0.0']
DEFAULT_RUNTIME = ['3.10.19', '3.11.14', '3.12.12', '3.13.12', '3.14.3']
DEFAULT_CYTHON = ["True", "False"]
matrices = [
  "FULL": [
    "SERVER": DEFAULT_CASSANDRA + DEFAULT_DSE,
    "RUNTIME": DEFAULT_RUNTIME,
    "CYTHON": DEFAULT_CYTHON
  ],
  "CASSANDRA": [
    "SERVER": DEFAULT_CASSANDRA,
    "RUNTIME": DEFAULT_RUNTIME,
    "CYTHON": DEFAULT_CYTHON
  ],
  "DSE": [
    "SERVER": DEFAULT_DSE,
    "RUNTIME": DEFAULT_RUNTIME,
    "CYTHON": DEFAULT_CYTHON
  ],
  "SMOKE": [
    "SERVER": DEFAULT_CASSANDRA.takeRight(2) + DEFAULT_DSE.takeRight(2) + DEFAULT_HCD.takeRight(1),
    "RUNTIME": DEFAULT_RUNTIME.take(1) + DEFAULT_RUNTIME.takeRight(1),
    "CYTHON": ["True"]
  ]
]

def initializeSlackContext() {
  /*
  Based on git branch/commit, configure the build context and env vars.
  */

  def driver_display_name = 'Cassandra Python Driver'
  if (env.GIT_URL.contains('riptano/python-driver')) {
    driver_display_name = 'private ' + driver_display_name
  } else if (env.GIT_URL.contains('python-dse-driver')) {
    driver_display_name = 'DSE Python Driver'
  }
  env.DRIVER_DISPLAY_NAME = driver_display_name
  env.GIT_SHA = "${env.GIT_COMMIT.take(7)}"
  env.GITHUB_PROJECT_URL = "https://${GIT_URL.replaceFirst(/(git@|http:\/\/|https:\/\/)/, '').replace(':', '/').replace('.git', '')}"
  env.GITHUB_BRANCH_URL = "${env.GITHUB_PROJECT_URL}/tree/${env.BRANCH_NAME}"
  env.GITHUB_COMMIT_URL = "${env.GITHUB_PROJECT_URL}/commit/${env.GIT_COMMIT}"
}

def getBuildContext() {
  /*
  Based on schedule and parameters, configure the build context and env vars.
  */

  def PROFILE = "${params.PROFILE}"
  def EVENT_LOOP = "${params.EVENT_LOOP.toLowerCase()}"

  matrixType = params.MATRIX != "DEFAULT" ? params.MATRIX : "SMOKE"
  matrix = matrices[matrixType].clone()

  // Check if parameters were set explicitly
  if (params.CYTHON != "DEFAULT") {
    matrix["CYTHON"] = [params.CYTHON]
  }

  if (params.SERVER_VERSION != "DEFAULT") {
    matrix["SERVER"] = [params.SERVER_VERSION]
  }

  if (params.PYTHON_VERSION != "DEFAULT") {
    matrix["RUNTIME"] = [params.PYTHON_VERSION]
  }

  if (params.CI_SCHEDULE == "WEEKNIGHTS") {
    matrix["SERVER"] = params.CI_SCHEDULE_SERVER_VERSION.split(' ')
    matrix["RUNTIME"] = params.CI_SCHEDULE_PYTHON_VERSION.split(' ')
  }

  context = [
    vars: [
      "PROFILE=${PROFILE}",
      "EVENT_LOOP=${EVENT_LOOP}"
    ],
    matrix: matrix
  ]

  return context
}

def buildAndTest(context) {
  initializeEnvironment()
  installDriver()

  try {
      executeTests()
    } finally {
      junit testResults: '*_results.xml'
  }
}

def getMatrixBuilds(buildContext) {
    def tasks = [:]
    matrix = buildContext.matrix

    matrix["SERVER"].each { serverVersion ->
      matrix["RUNTIME"].each { runtimeVersion ->
        matrix["CYTHON"].each { cythonFlag ->
          def taskVars = [
            "CASSANDRA_VERSION=${serverVersion}",
            "PYTHON_VERSION=${runtimeVersion}",
            "CYTHON_ENABLED=${cythonFlag}"
          ]
          def cythonDesc = cythonFlag == "True" ? ", Cython": ""
          tasks["${serverVersion}, py${runtimeVersion}${cythonDesc}"] = {
            node("${OS_VERSION}") {
              scm_variables = checkout scm
              env.GIT_COMMIT = scm_variables.get('GIT_COMMIT')
              env.GIT_URL = scm_variables.get('GIT_URL')
              initializeSlackContext()

              if (env.BUILD_STATED_SLACK_NOTIFIED != 'true') {
                slack.notifyChannel()
              }

              withEnv(taskVars) {
                buildAndTest(context)
              }
            }
          }
        }
      }
    }
    return tasks
}

def initializeEnvironment() {
  sh label: 'Initialize the environment', script: '''#!/bin/bash -lex

    # One of the integration tests relies on socat so let's install that here
    sudo apt-get install -y socat moreutils

    pyenv shell ${PYTHON_VERSION}
    python -m venv jenkins-venv
    . ./jenkins-venv/bin/activate
    pip install --upgrade pip setuptools wheel

    # Install a version of pyyaml<6.0 compatible with ccm-3.1.5 as of Aug 2023
    # this works around the python-3.10+ compatibility problem as described in DSP-23524
    pip install "pyyaml<6.0" --no-build-isolation
    pip install ${HOME}/ccm
  '''

  // Determine if server version is Apache CassandraⓇ or DataStax Enterprise
  if (env.CASSANDRA_VERSION.split('-')[0] == 'dse') {
    if (env.PYTHON_VERSION =~ /3\.12\.\d+/) {
      echo "Cannot install DSE dependencies for Python 3.12.x; installing Apache CassandraⓇ requirements only.  See PYTHON-1368 for more detail."
      sh label: 'Install Apache CassandraⓇ requirements', script: '''#!/bin/bash -lex
        . ./jenkins-venv/bin/activate
        pip install -r test-requirements.txt
      '''
    }
    else {
      sh label: 'Install DataStax Enterprise requirements', script: '''#!/bin/bash -lex
        . ./jenkins-venv/bin/activate
        pip install -r test-datastax-requirements.txt
      '''
    }
  } else {
    sh label: 'Install Apache CassandraⓇ requirements', script: '''#!/bin/bash -lex
      . ./jenkins-venv/bin/activate
      pip install -r test-requirements.txt
    '''

    sh label: 'Uninstall the geomet dependency since it is not required for Cassandra', script: '''#!/bin/bash -lex
      . ./jenkins-venv/bin/activate
      pip uninstall -y geomet
    '''
  }

  sh label: 'Download Apache CassandraⓇ or DataStax Enterprise', script: '''#!/bin/bash -lex
    . ${CCM_ENVIRONMENT_SHELL} ${CASSANDRA_VERSION}
  '''

  if (env.CASSANDRA_VERSION.split('-')[0] == 'dse') {
    env.DSE_FIXED_VERSION = env.CASSANDRA_VERSION.split('-')[1]
    sh label: 'Update environment for DataStax Enterprise', script: '''#!/bin/bash -le
        cat >> ${HOME}/environment.txt << ENVIRONMENT_EOF
CCM_CASSANDRA_VERSION=${DSE_FIXED_VERSION} # maintain for backwards compatibility
CCM_VERSION=${DSE_FIXED_VERSION}
CCM_SERVER_TYPE=dse
DSE_VERSION=${DSE_FIXED_VERSION}
CCM_IS_DSE=true
CCM_BRANCH=${DSE_FIXED_VERSION}
DSE_BRANCH=${DSE_FIXED_VERSION}
ENVIRONMENT_EOF
      '''
  } else if (env.CASSANDRA_VERSION.split('-')[0] == 'hcd') {
    env.HCD_FIXED_VERSION = env.CASSANDRA_VERSION.split('-')[1]
    sh label: 'Update environment for DataStax Enterprise', script: '''#!/bin/bash -le
        cat >> ${HOME}/environment.txt << ENVIRONMENT_EOF
CCM_CASSANDRA_VERSION=${HCD_FIXED_VERSION} # maintain for backwards compatibility
CCM_VERSION=${HCD_FIXED_VERSION}
CCM_SERVER_TYPE=hcd
HCD_VERSION=${HCD_FIXED_VERSION}
CCM_IS_HCD=true
CCM_BRANCH=${HCD_FIXED_VERSION}
HCD_BRANCH=${HCD_FIXED_VERSION}
ENVIRONMENT_EOF
      '''
  }

  sh label: 'Display Python and environment information', script: '''#!/bin/bash -le
    . ./jenkins-venv/bin/activate

    # Load CCM environment variables
    set -o allexport
    . ${HOME}/environment.txt
    set +o allexport

    python --version
    pip --version
    printenv | sort
  '''
}

def installDriver() {
  sh label: 'Install the driver and compile with C extensions with Cython', script: '''#!/bin/bash -lex
    # Update libev includes and libs to point to the right spot for this install
    pyenv shell ${PYTHON_VERSION}
    python -m venv libev-venv
    . ./libev-venv/bin/activate
    pip install toml
    python fix-jenkinsfile-libev.py ./pyproject.toml "/usr/include" "/usr/lib/x86_64-linux-gnu" | sponge ./pyproject.toml
    deactivate

    ls /usr/include/ev.h
    ls /usr/lib/x86_64-linux-gnu/libev*

    # Now that we've made relevant mods to our local pyproject.toml we're ready to build the driver
    . ./jenkins-venv/bin/activate

    # Load CCM environment variables
    set -o allexport
    . ${HOME}/environment.txt
    set +o allexport

    cat ./pyproject.toml
    pip install --verbose --editable .

    # After install display a list of packages in the venv for auditing
    pip list
  '''
}


def executeStandardTests() {

  try {
    sh label: 'Execute unit tests', script: '''#!/bin/bash -lex
      . ./jenkins-venv/bin/activate

      # Load CCM environment variables
      set -o allexport
      . ${HOME}/environment.txt
      set +o allexport

      failure=0
      EVENT_LOOP=${EVENT_LOOP} VERIFY_CYTHON=${CYTHON_ENABLED} JVM_EXTRA_OPTS="$JVM_EXTRA_OPTS -Xss384k" pytest -s -v --log-format="[%(levelname)s] %(asctime)s %(thread)d: %(message)s" --junit-xml=unit_results.xml tests/unit/ || failure=1
      exit $failure
    '''
  } catch (err) {
    currentBuild.result = 'UNSTABLE'
  }

  try {
    sh label: 'Execute Simulacron integration tests', script: '''#!/bin/bash -lex
      . ./jenkins-venv/bin/activate

      # Load CCM environment variables
      set -o allexport
      . ${HOME}/environment.txt
      set +o allexport

      . ${JABBA_SHELL}
      jabba use 1.8

      failure=0
      SIMULACRON_JAR="${HOME}/simulacron.jar"
      SIMULACRON_JAR=${SIMULACRON_JAR} EVENT_LOOP=${EVENT_LOOP} CASSANDRA_DIR=${CCM_INSTALL_DIR} CCM_ARGS="${CCM_ARGS}" DSE_VERSION=${DSE_VERSION} HCD_VERSION=${HCD_VERSION} CASSANDRA_VERSION=${CCM_CASSANDRA_VERSION} MAPPED_CASSANDRA_VERSION=${MAPPED_CASSANDRA_VERSION} VERIFY_CYTHON=${CYTHON_ENABLED} JVM_EXTRA_OPTS="$JVM_EXTRA_OPTS -Xss384k" pytest -s -v --log-format="[%(levelname)s] %(asctime)s %(thread)d: %(message)s" --ignore=test_backpressure.py --junit-xml=simulacron_results.xml tests/integration/simulacron/ || true

      # Run backpressure tests separately to avoid memory issue
      SIMULACRON_JAR=${SIMULACRON_JAR} EVENT_LOOP=${EVENT_LOOP} CASSANDRA_DIR=${CCM_INSTALL_DIR} CCM_ARGS="${CCM_ARGS}" DSE_VERSION=${DSE_VERSION} HCD_VERSION=${HCD_VERSION} CASSANDRA_VERSION=${CCM_CASSANDRA_VERSION} MAPPED_CASSANDRA_VERSION=${MAPPED_CASSANDRA_VERSION} VERIFY_CYTHON=${CYTHON_ENABLED} JVM_EXTRA_OPTS="$JVM_EXTRA_OPTS -Xss384k" pytest -s -v --log-format="[%(levelname)s] %(asctime)s %(thread)d: %(message)s" --ignore=test_backpressure.py --junit-xml=simulacron_backpressure_1_results.xml tests/integration/simulacron/test_backpressure.py:TCPBackpressureTests.test_paused_connections || failure=1
      SIMULACRON_JAR=${SIMULACRON_JAR} EVENT_LOOP=${EVENT_LOOP} CASSANDRA_DIR=${CCM_INSTALL_DIR} CCM_ARGS="${CCM_ARGS}" DSE_VERSION=${DSE_VERSION} HCD_VERSION=${HCD_VERSION} CASSANDRA_VERSION=${CCM_CASSANDRA_VERSION} MAPPED_CASSANDRA_VERSION=${MAPPED_CASSANDRA_VERSION} VERIFY_CYTHON=${CYTHON_ENABLED} JVM_EXTRA_OPTS="$JVM_EXTRA_OPTS -Xss384k" pytest -s -v --log-format="[%(levelname)s] %(asctime)s %(thread)d: %(message)s" --ignore=test_backpressure.py --junit-xml=simulacron_backpressure_2_results.xml tests/integration/simulacron/test_backpressure.py:TCPBackpressureTests.test_queued_requests_timeout || failure=1
      SIMULACRON_JAR=${SIMULACRON_JAR} EVENT_LOOP=${EVENT_LOOP} CASSANDRA_DIR=${CCM_INSTALL_DIR} CCM_ARGS="${CCM_ARGS}" DSE_VERSION=${DSE_VERSION} HCD_VERSION=${HCD_VERSION} CASSANDRA_VERSION=${CCM_CASSANDRA_VERSION} MAPPED_CASSANDRA_VERSION=${MAPPED_CASSANDRA_VERSION} VERIFY_CYTHON=${CYTHON_ENABLED} JVM_EXTRA_OPTS="$JVM_EXTRA_OPTS -Xss384k" pytest -s -v --log-format="[%(levelname)s] %(asctime)s %(thread)d: %(message)s" --ignore=test_backpressure.py --junit-xml=simulacron_backpressure_3_results.xml tests/integration/simulacron/test_backpressure.py:TCPBackpressureTests.test_cluster_busy || failure=1
      SIMULACRON_JAR=${SIMULACRON_JAR} EVENT_LOOP=${EVENT_LOOP} CASSANDRA_DIR=${CCM_INSTALL_DIR} CCM_ARGS="${CCM_ARGS}" DSE_VERSION=${DSE_VERSION} HCD_VERSION=${HCD_VERSION} CASSANDRA_VERSION=${CCM_CASSANDRA_VERSION} MAPPED_CASSANDRA_VERSION=${MAPPED_CASSANDRA_VERSION} VERIFY_CYTHON=${CYTHON_ENABLED} JVM_EXTRA_OPTS="$JVM_EXTRA_OPTS -Xss384k" pytest -s -v --log-format="[%(levelname)s] %(asctime)s %(thread)d: %(message)s" --ignore=test_backpressure.py --junit-xml=simulacron_backpressure_4_results.xml tests/integration/simulacron/test_backpressure.py:TCPBackpressureTests.test_node_busy || failure=1
      exit $failure
    '''
  } catch (err) {
    currentBuild.result = 'UNSTABLE'
  }

  try {
    sh label: 'Execute CQL engine integration tests', script: '''#!/bin/bash -lex
      . ./jenkins-venv/bin/activate

      # Load CCM environment variables
      set -o allexport
      . ${HOME}/environment.txt
      set +o allexport

      . ${JABBA_SHELL}
      jabba use 1.8

      EVENT_LOOP=${EVENT_LOOP} CCM_ARGS="${CCM_ARGS}" DSE_VERSION=${DSE_VERSION} HCD_VERSION=${HCD_VERSION} CASSANDRA_VERSION=${CCM_CASSANDRA_VERSION} MAPPED_CASSANDRA_VERSION=${MAPPED_CASSANDRA_VERSION} VERIFY_CYTHON=${CYTHON_ENABLED} JVM_EXTRA_OPTS="$JVM_EXTRA_OPTS -Xss384k" pytest -s -v --log-format="[%(levelname)s] %(asctime)s %(thread)d: %(message)s" --junit-xml=cqle_results.xml tests/integration/cqlengine/
    '''
  } catch (err) {
    currentBuild.result = 'UNSTABLE'
  }

  try {
    sh label: 'Execute Apache CassandraⓇ integration tests', script: '''#!/bin/bash -lex
      . ./jenkins-venv/bin/activate

      # Load CCM environment variables
      set -o allexport
      . ${HOME}/environment.txt
      set +o allexport

      . ${JABBA_SHELL}
      jabba use 1.8

      EVENT_LOOP=${EVENT_LOOP} CCM_ARGS="${CCM_ARGS}" DSE_VERSION=${DSE_VERSION} HCD_VERSION=${HCD_VERSION} CASSANDRA_VERSION=${CCM_CASSANDRA_VERSION} MAPPED_CASSANDRA_VERSION=${MAPPED_CASSANDRA_VERSION} VERIFY_CYTHON=${CYTHON_ENABLED} JVM_EXTRA_OPTS="$JVM_EXTRA_OPTS -Xss384k" pytest -s -v --log-format="[%(levelname)s] %(asctime)s %(thread)d: %(message)s" --junit-xml=standard_results.xml tests/integration/standard/
    '''
  } catch (err) {
    currentBuild.result = 'UNSTABLE'
  }

  if (env.CASSANDRA_VERSION.split('-')[0] == 'dse' && env.CASSANDRA_VERSION.split('-')[1] != '4.8') {
    if (env.PYTHON_VERSION =~ /3\.12\.\d+/) {
      echo "Cannot install DSE dependencies for Python 3.12.x.  See PYTHON-1368 for more detail."
    }
    else {
      try {
        sh label: 'Execute DataStax Enterprise integration tests', script: '''#!/bin/bash -lex
          . ./jenkins-venv/bin/activate

          # Load CCM environment variable
          set -o allexport
          . ${HOME}/environment.txt
          set +o allexport

          . ${JABBA_SHELL}
          jabba use 1.8

          EVENT_LOOP=${EVENT_LOOP} CASSANDRA_DIR=${CCM_INSTALL_DIR} DSE_VERSION=${DSE_VERSION} HCD_VERSION=${HCD_VERSION} ADS_HOME="${HOME}/" VERIFY_CYTHON=${CYTHON_ENABLED} JVM_EXTRA_OPTS="$JVM_EXTRA_OPTS -Xss384k" pytest -s -v --log-format="[%(levelname)s] %(asctime)s %(thread)d: %(message)s" --junit-xml=dse_results.xml tests/integration/advanced/
        '''
      } catch (err) {
        currentBuild.result = 'UNSTABLE'
      }
    }
  }

  if (env.PROFILE == 'FULL') {
    try {
      sh label: 'Execute long running integration tests', script: '''#!/bin/bash -lex
        . ./jenkins-venv/bin/activate

        # Load CCM environment variable
        set -o allexport
        . ${HOME}/environment.txt
        set +o allexport

        . ${JABBA_SHELL}
        jabba use 1.8

        EVENT_LOOP=${EVENT_LOOP} CCM_ARGS="${CCM_ARGS}" DSE_VERSION=${DSE_VERSION} HCD_VERSION=${HCD_VERSION} CASSANDRA_VERSION=${CCM_CASSANDRA_VERSION} MAPPED_CASSANDRA_VERSION=${MAPPED_CASSANDRA_VERSION} VERIFY_CYTHON=${CYTHON_ENABLED} JVM_EXTRA_OPTS="$JVM_EXTRA_OPTS -Xss384k" pytest -s -v --log-format="[%(levelname)s] %(asctime)s %(thread)d: %(message)s" --ignore=tests/integration/long/upgrade --junit-xml=long_results.xml tests/integration/long/
      '''
    } catch (err) {
      currentBuild.result = 'UNSTABLE'
    }
  }
}

def executeDseSmokeTests() {
  sh label: 'Execute profile DataStax Enterprise smoke test integration tests', script: '''#!/bin/bash -lex
    . ./jenkins-venv/bin/activate

    # Load CCM environment variable
    set -o allexport
    . ${HOME}/environment.txt
    set +o allexport

    . ${JABBA_SHELL}
    jabba use 1.8

    EVENT_LOOP=${EVENT_LOOP} CCM_ARGS="${CCM_ARGS}" CASSANDRA_VERSION=${CCM_CASSANDRA_VERSION} DSE_VERSION=${DSE_VERSION} MAPPED_CASSANDRA_VERSION=${MAPPED_CASSANDRA_VERSION} VERIFY_CYTHON=${CYTHON_ENABLED} JVM_EXTRA_OPTS="$JVM_EXTRA_OPTS -Xss384k" pytest -s -v --log-format="[%(levelname)s] %(asctime)s %(thread)d: %(message)s" --junit-xml=standard_results.xml tests/integration/standard/test_dse.py
  '''
}

def executeEventLoopTests() {
  sh label: 'Execute profile event loop manager integration tests', script: '''#!/bin/bash -lex
    . ./jenkins-venv/bin/activate

    # Load CCM environment variable
    set -o allexport
    . ${HOME}/environment.txt
    set +o allexport

    . ${JABBA_SHELL}
    jabba use 1.8

    EVENT_LOOP_TESTS=(
      "tests/integration/standard/test_cluster.py"
      "tests/integration/standard/test_concurrent.py"
      "tests/integration/standard/test_connection.py"
      "tests/integration/standard/test_control_connection.py"
      "tests/integration/standard/test_metrics.py"
      "tests/integration/standard/test_query.py"
      "tests/integration/simulacron/test_endpoint.py"
      "tests/integration/long/test_ssl.py"
    )
    EVENT_LOOP=${EVENT_LOOP} CCM_ARGS="${CCM_ARGS}" DSE_VERSION=${DSE_VERSION} HCD_VERSION=${HCD_VERSION} CASSANDRA_VERSION=${CCM_CASSANDRA_VERSION} MAPPED_CASSANDRA_VERSION=${MAPPED_CASSANDRA_VERSION} VERIFY_CYTHON=${CYTHON_ENABLED} JVM_EXTRA_OPTS="$JVM_EXTRA_OPTS -Xss384k" pytest -s -v --log-format="[%(levelname)s] %(asctime)s %(thread)d: %(message)s" --junit-xml=standard_results.xml ${EVENT_LOOP_TESTS[@]}
  '''
}

def executeTests() {
  switch(env.PROFILE) {
    case 'DSE-SMOKE-TEST':
      executeDseSmokeTests()
      break
    case 'EVENT_LOOP':
      executeEventLoopTests()
      break
    default:
      executeStandardTests()
      break
  }
}


// TODO move this in the shared lib
def getDriverMetricType() {
  metric_type = 'oss'
  if (env.GIT_URL.contains('riptano/python-driver')) {
    metric_type = 'oss-private'
  } else if (env.GIT_URL.contains('python-dse-driver')) {
    metric_type = 'dse'
  }
  return metric_type
}

def describeBuild(buildContext) {
  script {
    def runtimes = buildContext.matrix["RUNTIME"]
    def serverVersions = buildContext.matrix["SERVER"]
    def numBuilds = runtimes.size() * serverVersions.size() * buildContext.matrix["CYTHON"].size()
    currentBuild.displayName = "${env.PROFILE} (${env.EVENT_LOOP} | ${numBuilds} builds)"
    currentBuild.description = "${env.PROFILE} build testing servers (${serverVersions.join(', ')}) against Python (${runtimes.join(', ')}) using ${env.EVENT_LOOP} event loop manager"
  }
}

// branch pattern for cron
def branchPatternCron() {
  ~"(master)"
}

pipeline {
  agent none

  // Global pipeline timeout
  options {
    disableConcurrentBuilds()
    timeout(time: 10, unit: 'HOURS') // TODO timeout should be per build
    buildDiscarder(logRotator(artifactNumToKeepStr: '10', // Keep only the last 10 artifacts
                              numToKeepStr: '50'))        // Keep only the last 50 build records
  }

  parameters {
    choice(
      name: 'ADHOC_BUILD_TYPE',
      choices: ['BUILD', 'BUILD-AND-EXECUTE-TESTS'],
      description: '''<p>Perform a adhoc build operation</p>
                      <table style="width:100%">
                        <col width="25%">
                        <col width="75%">
                        <tr>
                          <th align="left">Choice</th>
                          <th align="left">Description</th>
                        </tr>
                        <tr>
                          <td><strong>BUILD</strong></td>
                          <td>Performs a <b>Per-Commit</b> build</td>
                        </tr>
                        <tr>
                          <td><strong>BUILD-AND-EXECUTE-TESTS</strong></td>
                          <td>Performs a build and executes the integration and unit tests</td>
                        </tr>
                      </table>''')
    choice(
      name: 'PROFILE',
      choices: ['STANDARD', 'FULL', 'DSE-SMOKE-TEST', 'EVENT_LOOP'],
      description: '''<p>Profile to utilize for scheduled or adhoc builds</p>
                      <table style="width:100%">
                        <col width="25%">
                        <col width="75%">
                        <tr>
                          <th align="left">Choice</th>
                          <th align="left">Description</th>
                        </tr>
                        <tr>
                          <td><strong>STANDARD</strong></td>
                          <td>Execute the standard tests for the driver</td>
                        </tr>
                        <tr>
                          <td><strong>FULL</strong></td>
                          <td>Execute all tests for the driver, including long tests.</td>
                        </tr>
                        <tr>
                          <td><strong>DSE-SMOKE-TEST</strong></td>
                          <td>Execute only the DataStax Enterprise smoke tests</td>
                        </tr>
                        <tr>
                          <td><strong>EVENT_LOOP</strong></td>
                          <td>Execute only the event loop tests for the specified event loop manager (see: <b>EVENT_LOOP</b>)</td>
                        </tr>
                      </table>''')
    choice(
      name: 'MATRIX',
      choices: ['DEFAULT', 'SMOKE', 'FULL', 'CASSANDRA', 'DSE', 'HCD'],
      description: '''<p>The matrix for the build.</p>
                      <table style="width:100%">
                        <col width="25%">
                        <col width="75%">
                        <tr>
                          <th align="left">Choice</th>
                          <th align="left">Description</th>
                        </tr>
                        <tr>
                          <td><strong>DEFAULT</strong></td>
                          <td>Default to the build context.</td>
                        </tr>
                        <tr>
                          <td><strong>SMOKE</strong></td>
                          <td>Basic smoke tests for current Python runtimes + C*/DSE versions, no Cython</td>
                        </tr>
                        <tr>
                          <td><strong>FULL</strong></td>
                          <td>All server versions, python runtimes tested with and without Cython.</td>
                        </tr>
                        <tr>
                          <td><strong>CASSANDRA</strong></td>
                          <td>All cassandra server versions.</td>
                        </tr>
                        <tr>
                          <td><strong>DSE</strong></td>
                          <td>All dse server versions.</td>
                        </tr>
                        <tr>
                          <td><strong>HCD</strong></td>
                          <td>All hcd server versions.</td>
                        </tr>
                      </table>''')
    choice(
      name: 'PYTHON_VERSION',
      choices: ['DEFAULT'] + DEFAULT_RUNTIME,
      description: 'Python runtime version. Default to the build context.')
    choice(
      name: 'SERVER_VERSION',
      choices: ['DEFAULT'] + DEFAULT_CASSANDRA + DEFAULT_DSE + DEFAULT_HCD,
      description: '''Apache CassandraⓇ and DataStax Enterprise server version to use for adhoc <b>BUILD-AND-EXECUTE-TESTS</b> <strong>ONLY!</strong>
                      <table style="width:100%">
                        <col width="15%">
                        <col width="85%">
                        <tr>
                          <th align="left">Choice</th>
                          <th align="left">Description</th>
                        </tr>
                         <tr>
                          <td><strong>DEFAULT</strong></td>
                          <td>Default to the build context.</td>
                        </tr>
                        <tr>
                          <td><strong>3.0</strong></td>
                          <td>Apache CassandraⓇ v3.0.x</td>
                        </tr>
                        <tr>
                          <td><strong>3.11</strong></td>
                          <td>Apache CassandraⓇ v3.11.x</td>
                        </tr>
                        <tr>
                          <td><strong>4.0</strong></td>
                          <td>Apache CassandraⓇ v4.0.x</td>
                        </tr>
                        <tr>
                          <td><strong>5.0</strong></td>
                          <td>Apache CassandraⓇ v5.0.x</td>
                        </tr>
                        <tr>
                          <td><strong>dse-5.1.35</strong></td>
                          <td>DataStax Enterprise v5.1.x</td>
                        </tr>
                        <tr>
                          <td><strong>dse-6.8.30</strong></td>
                          <td>DataStax Enterprise v6.8.x</td>
                        </tr>
                        <tr>
                          <td><strong>dse-6.9.0</strong></td>
                          <td>DataStax Enterprise v6.9.x (<b>CURRENTLY UNDER DEVELOPMENT</b>)</td>
                        </tr>
                        <tr>
                          <td><strong>hcd-1.0.0</strong></td>
                          <td>DataStax HCD v1.0.x (<b>CURRENTLY UNDER DEVELOPMENT</b>)</td>
                        </tr>
                      </table>''')
    choice(
      name: 'CYTHON',
      choices: ['DEFAULT'] + DEFAULT_CYTHON,
      description: '''<p>Flag to determine if Cython should be enabled</p>
                      <table style="width:100%">
                        <col width="25%">
                        <col width="75%">
                        <tr>
                          <th align="left">Choice</th>
                          <th align="left">Description</th>
                        </tr>
                        <tr>
                          <td><strong>Default</strong></td>
                          <td>Default to the build context.</td>
                        </tr>
                        <tr>
                          <td><strong>True</strong></td>
                          <td>Enable Cython</td>
                        </tr>
                        <tr>
                          <td><strong>False</strong></td>
                          <td>Disable Cython</td>
                        </tr>
                      </table>''')
    choice(
      name: 'EVENT_LOOP',
      choices: ['LIBEV', 'ASYNCIO', 'ASYNCORE'],
      description: '''<p>Event loop manager to utilize for scheduled or adhoc builds</p>
                      <table style="width:100%">
                        <col width="25%">
                        <col width="75%">
                        <tr>
                          <th align="left">Choice</th>
                          <th align="left">Description</th>
                        </tr>
                        <tr>
                          <td><strong>LIBEV</strong></td>
                          <td>A full-featured and high-performance event loop that is loosely modeled after libevent, but without its limitations and bugs</td>
                        </tr>
                        <tr>
                          <td><strong>ASYNCIO</strong></td>
                          <td>A library to write concurrent code using the async/await syntax</td>
                        </tr>
                        <tr>
                          <td><strong>ASYNCORE</strong></td>
                          <td>A module provides the basic infrastructure for writing asynchronous socket service clients and servers</td>
                        </tr>
                      </table>''')
    choice(
      name: 'CI_SCHEDULE',
      choices: ['DO-NOT-CHANGE-THIS-SELECTION', 'WEEKNIGHTS', 'WEEKENDS'],
      description: 'CI testing schedule to execute periodically scheduled builds and tests of the driver (<strong>DO NOT CHANGE THIS SELECTION</strong>)')
    string(
      name: 'CI_SCHEDULE_PYTHON_VERSION',
      defaultValue: 'DO-NOT-CHANGE-THIS-SELECTION',
      description: 'CI testing python version to utilize for scheduled test runs of the driver (<strong>DO NOT CHANGE THIS SELECTION</strong>)')
    string(
      name: 'CI_SCHEDULE_SERVER_VERSION',
      defaultValue: 'DO-NOT-CHANGE-THIS-SELECTION',
      description: 'CI testing server version to utilize for scheduled test runs of the driver (<strong>DO NOT CHANGE THIS SELECTION</strong>)')
  }

  triggers {
    parameterizedCron(branchPatternCron().matcher(env.BRANCH_NAME).matches() ? """
      # Every weeknight (Monday - Friday) around 4:00 AM
      # These schedules will run with and without Cython enabled for Python 3.9.23 and 3.13.5
      H 4 * * 1-5 %CI_SCHEDULE=WEEKNIGHTS;EVENT_LOOP=LIBEV;CI_SCHEDULE_PYTHON_VERSION=3.9.23 3.13.5;CI_SCHEDULE_SERVER_VERSION=3.11 4.0 5.0 dse-5.1.35 dse-6.8.30 dse-6.9.0 hcd-1.0.0
    """ : "")
  }

  environment {
    OS_VERSION = 'ubuntu/jammy64/python-driver'
    CCM_ENVIRONMENT_SHELL = '/usr/local/bin/ccm_environment.sh'
    CCM_MAX_HEAP_SIZE = '1536M'
    JABBA_SHELL = '/usr/lib/jabba/jabba.sh'
  }

  stages {
    stage ('Build and Test') {
      when {
        beforeAgent true
        allOf {
          not { buildingTag() }
        }
      }

      steps {
        script {
          context = getBuildContext()
          withEnv(context.vars) {
            describeBuild(context)            

            // build and test all builds
            parallel getMatrixBuilds(context)

            slack.notifyChannel(currentBuild.currentResult)
          }
        }
      }
    }

  }
}
