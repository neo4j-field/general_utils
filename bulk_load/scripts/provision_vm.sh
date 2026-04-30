#!/usr/bin/env bash
#
# provision_vm.sh
# Idempotent provisioning of a Debian/Ubuntu VM for the Neo4j bulk load
# benchmark. Installs Java 17, Python 3, cypher-shell, and the Neo4j
# Spark Connector jar. Safe to re-run.
#
# Designed for: GCP Compute Engine, n2-standard-8, Debian 12.
# Should also work on Ubuntu 22.04+ with apt.
#
# Usage:
#   ./provision_vm.sh
#
# Tunable env vars:
#   JAVA_VERSION       Default: 17
#   PYTHON_VERSION     Default: 3 (system default)
#   SPARK_CONNECTOR_VERSION  Default: 5.3.10_for_spark_3
#   SCALA_VERSION      Default: 2.12 (matches PySpark 3.5 default bundle)
#   JARS_DIR           Default: $HOME/jars
#   VENV_DIR           Default: $HOME/.venv-bulkload

set -euo pipefail

JAVA_VERSION="${JAVA_VERSION:-17}"
SPARK_CONNECTOR_VERSION="${SPARK_CONNECTOR_VERSION:-5.3.10_for_spark_3}"
SCALA_VERSION="${SCALA_VERSION:-2.12}"
JARS_DIR="${JARS_DIR:-$HOME/jars}"
VENV_DIR="${VENV_DIR:-$HOME/.venv-bulkload}"

log() { echo "[provision] $*"; }

# ---------- 1. System packages ----------
log "Updating apt and installing base packages..."
sudo apt-get update -qq
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq \
    "openjdk-${JAVA_VERSION}-jdk-headless" \
    python3 python3-pip python3-venv \
    git curl wget unzip ca-certificates jq

JAVA_HOME_PATH="$(dirname "$(dirname "$(readlink -f "$(which java)")")")"
log "Java installed at: $JAVA_HOME_PATH"

# ---------- 2. cypher-shell (standalone zip, no apt deps) ----------
# The apt package pulls a JRE that conflicts with the JDK we just installed.
# The standalone zip is self-contained and only needs a JVM (which we have).
CYPHER_SHELL_VERSION="${CYPHER_SHELL_VERSION:-5.26.0}"
CYPHER_SHELL_DIR="$HOME/cypher-shell"
if ! command -v cypher-shell >/dev/null 2>&1; then
    log "Installing cypher-shell ${CYPHER_SHELL_VERSION} (standalone zip)..."
    TMPZIP="$(mktemp --suffix=.zip)"
    if curl -fsSL -o "$TMPZIP" \
        "https://dist.neo4j.org/cypher-shell/cypher-shell-${CYPHER_SHELL_VERSION}.zip"; then
        rm -rf "$CYPHER_SHELL_DIR"
        # The zip extracts to a directory literally named "cypher-shell/" containing
        # bin/cypher-shell, lib/, and license files. We move it to a stable HOME path
        # and symlink the launcher in bin/ to /usr/local/bin.
        unzip -q "$TMPZIP" -d "$HOME"
        sudo ln -sf "$CYPHER_SHELL_DIR/bin/cypher-shell" /usr/local/bin/cypher-shell
        rm -f "$TMPZIP"
    else
        log "WARN: cypher-shell download failed. Skipping (the Python loader does not need it)."
    fi
else
    log "cypher-shell already installed: $(cypher-shell --version 2>&1 | head -1)"
fi
# Verify is best-effort: the Python loader does not require cypher-shell.
command -v cypher-shell >/dev/null 2>&1 \
    && log "cypher-shell: $(cypher-shell --version 2>&1 | head -1)" \
    || log "cypher-shell not installed (ok, the Python loader does not need it)"

# ---------- 3. Neo4j Spark Connector jar ----------
mkdir -p "$JARS_DIR"
CONNECTOR_JAR="neo4j-connector-apache-spark_${SCALA_VERSION}-${SPARK_CONNECTOR_VERSION}.jar"
CONNECTOR_URL="https://repo1.maven.org/maven2/org/neo4j/neo4j-connector-apache-spark_${SCALA_VERSION}/${SPARK_CONNECTOR_VERSION}/${CONNECTOR_JAR}"

if [[ ! -f "$JARS_DIR/$CONNECTOR_JAR" ]]; then
    log "Downloading Neo4j Spark Connector: $CONNECTOR_JAR"
    curl -fsSL -o "$JARS_DIR/$CONNECTOR_JAR" "$CONNECTOR_URL"
else
    log "Spark Connector jar already present: $JARS_DIR/$CONNECTOR_JAR"
fi

# ---------- 4. Python virtualenv + deps ----------
if [[ ! -d "$VENV_DIR" ]]; then
    log "Creating Python virtualenv at $VENV_DIR"
    python3 -m venv "$VENV_DIR"
fi

# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"
pip install --quiet --upgrade pip
pip install --quiet \
    "pyspark==3.5.4" \
    "pyarrow>=15.0.0" \
    "numpy>=1.26.0" \
    "pyyaml>=6.0.0" \
    "python-dotenv>=1.0.0" \
    "neo4j>=5.27.0"
log "Python deps installed in $VENV_DIR"

# ---------- 5. Environment hints ----------
PROFILE_LINE="export NEO4J_SPARK_CONNECTOR_JAR=\"$JARS_DIR/$CONNECTOR_JAR\""
if ! grep -qF "$PROFILE_LINE" "$HOME/.bashrc" 2>/dev/null; then
    {
        echo ""
        echo "# Neo4j bulk load benchmark"
        echo "$PROFILE_LINE"
        echo "alias activate-bulkload='source $VENV_DIR/bin/activate'"
    } >> "$HOME/.bashrc"
fi

# ---------- 6. Verify ----------
log "Verifying versions..."
java -version 2>&1 | head -1
python3 --version
"$VENV_DIR/bin/python" -c "import pyspark; print(f'pyspark {pyspark.__version__}')"
"$VENV_DIR/bin/python" -c "import pyarrow; print(f'pyarrow {pyarrow.__version__}')"
"$VENV_DIR/bin/python" -c "import neo4j; print(f'neo4j-python {neo4j.__version__}')"
ls -lh "$JARS_DIR/$CONNECTOR_JAR" | awk '{print "Spark Connector:", $5, $9}'

log "Provisioning complete."
log "Next: 'source ~/.bashrc && activate-bulkload' to enter the Python venv."
