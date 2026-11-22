#!/usr/bin/env bash
# ==============================================================================
# Setup Apache Hadoop 3.3.6 (Single-Node Pseudo-Distributed) on Ubuntu (WSL2)
# Course: CSE3712 — Big Data Analytics
# ==============================================================================
set -e

echo "=== [1/7] Installing Dependencies (Java OpenJDK 11, SSH, rsync, curl) ==="
sudo apt-get update -y
sudo apt-get install -y openjdk-11-jdk ssh pdsh curl rsync python-is-python3 || sudo apt-get install -y openjdk-11-jdk ssh pdsh curl rsync

# Symlink python to python3 if missing
if ! command -v python &> /dev/null && command -v python3 &> /dev/null; then
    sudo ln -s /usr/bin/python3 /usr/bin/python
fi

# Detect JAVA_HOME
JAVA_BIN=$(readlink -f /usr/bin/java)
DETECTED_JAVA_HOME=$(dirname $(dirname "$JAVA_BIN"))
export JAVA_HOME="$DETECTED_JAVA_HOME"
echo "Detected JAVA_HOME: $JAVA_HOME"

if ! grep -q "JAVA_HOME" ~/.bashrc; then
    echo "export JAVA_HOME=$JAVA_HOME" >> ~/.bashrc
fi

echo "=== [2/7] Configuring Passwordless SSH ==="
sudo ssh-keygen -A 2>/dev/null || true
sudo service ssh start || true

mkdir -p ~/.ssh
chmod 700 ~/.ssh

if [ ! -f ~/.ssh/id_rsa ]; then
    ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
fi
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys

cat << 'EOF' > ~/.ssh/config
Host *
    StrictHostKeyChecking no
    UserKnownHostsFile /dev/null
    LogLevel ERROR
EOF
chmod 600 ~/.ssh/config

echo "=== [3/7] Setting up Apache Hadoop 3.3.6 ==="
HADOOP_DIR="$HOME/hadoop-3.3.6"
if [ ! -d "$HADOOP_DIR" ]; then
    cd "$HOME"
    if [ ! -f "hadoop-3.3.6.tar.gz" ]; then
        echo "Downloading hadoop-3.3.6.tar.gz..."
        curl -f -L -O https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz || \
        curl -f -L -O https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
    fi
    echo "Extracting hadoop-3.3.6.tar.gz..."
    tar -xzf hadoop-3.3.6.tar.gz
fi

export HADOOP_HOME="$HADOOP_DIR"
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$JAVA_HOME/bin:$PATH"

if ! grep -q "HADOOP_HOME" ~/.bashrc; then
    echo "export HADOOP_HOME=$HADOOP_DIR" >> ~/.bashrc
    echo "export HADOOP_CONF_DIR=\$HADOOP_HOME/etc/hadoop" >> ~/.bashrc
    echo 'export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$JAVA_HOME/bin:$PATH"' >> ~/.bashrc
fi

echo "=== [4/7] Configuring Hadoop Environment & XML Profiles ==="
# hadoop-env.sh
sed -i '/export JAVA_HOME/d' "$HADOOP_CONF_DIR/hadoop-env.sh"
echo "export JAVA_HOME=$JAVA_HOME" >> "$HADOOP_CONF_DIR/hadoop-env.sh"
echo "export HDFS_NAMENODE_USER=$USER" >> "$HADOOP_CONF_DIR/hadoop-env.sh"
echo "export HDFS_DATANODE_USER=$USER" >> "$HADOOP_CONF_DIR/hadoop-env.sh"
echo "export HDFS_SECONDARYNAMENODE_USER=$USER" >> "$HADOOP_CONF_DIR/hadoop-env.sh"
echo "export YARN_RESOURCEMANAGER_USER=$USER" >> "$HADOOP_CONF_DIR/hadoop-env.sh"
echo "export YARN_NODEMANAGER_USER=$USER" >> "$HADOOP_CONF_DIR/hadoop-env.sh"
echo 'export HADOOP_OPTS="$HADOOP_OPTS -Xrs"' >> "$HADOOP_CONF_DIR/hadoop-env.sh"
echo 'export YARN_OPTS="$YARN_OPTS -Xrs"' >> "$HADOOP_CONF_DIR/hadoop-env.sh"

# Persistent storage directories under user home
HDFS_NAME_DIR="$HOME/hadoop-data/hdfs/namenode"
HDFS_DATA_DIR="$HOME/hadoop-data/hdfs/datanode"
mkdir -p "$HDFS_NAME_DIR" "$HDFS_DATA_DIR"

# core-site.xml
cat << EOF > "$HADOOP_CONF_DIR/core-site.xml"
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
    </property>
</configuration>
EOF

# hdfs-site.xml
cat << EOF > "$HADOOP_CONF_DIR/hdfs-site.xml"
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>file://${HDFS_NAME_DIR}</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>file://${HDFS_DATA_DIR}</value>
    </property>
</configuration>
EOF

# mapred-site.xml
cat << EOF > "$HADOOP_CONF_DIR/mapred-site.xml"
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
    <property>
        <name>yarn.app.mapreduce.am.env</name>
        <value>HADOOP_MAPRED_HOME=${HADOOP_DIR}</value>
    </property>
    <property>
        <name>mapreduce.map.env</name>
        <value>HADOOP_MAPRED_HOME=${HADOOP_DIR}</value>
    </property>
    <property>
        <name>mapreduce.reduce.env</name>
        <value>HADOOP_MAPRED_HOME=${HADOOP_DIR}</value>
    </property>
</configuration>
EOF

# yarn-site.xml
cat << EOF > "$HADOOP_CONF_DIR/yarn-site.xml"
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>
    <property>
        <name>yarn.nodemanager.aux-services.mapreduce.shuffle.class</name>
        <value>org.apache.hadoop.mapred.ShuffleHandler</value>
    </property>
    <property>
        <name>yarn.nodemanager.env-whitelist</name>
        <value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_HOME,PATH,LANG,LC_ALL</value>
    </property>
    <property>
        <name>yarn.nodemanager.vmem-check-enabled</name>
        <value>false</value>
    </property>
    <property>
        <name>yarn.resourcemanager.hostname</name>
        <value>localhost</value>
    </property>
    <property>
        <name>yarn.resourcemanager.address</name>
        <value>localhost:8032</value>
    </property>
    <property>
        <name>yarn.resourcemanager.scheduler.address</name>
        <value>localhost:8030</value>
    </property>
    <property>
        <name>yarn.resourcemanager.resource-tracker.address</name>
        <value>localhost:8031</value>
    </property>
</configuration>
EOF

echo "=== [5/7] Checking HDFS NameNode Initialization ==="
# Only format if this is a genuinely new setup with empty storage directory
if [ ! -d "$HDFS_NAME_DIR/current" ]; then
    echo "Initializing new NameNode at $HDFS_NAME_DIR..."
    "$HADOOP_HOME/bin/hdfs" namenode -format -force
else
    echo "Existing NameNode metadata detected at $HDFS_NAME_DIR. Preserving storage without reformatting."
fi

echo "=== [6/7] Starting HDFS & YARN Daemons ==="
sudo service ssh start || true
"$HADOOP_HOME/bin/hdfs" --daemon start namenode
"$HADOOP_HOME/bin/hdfs" --daemon start datanode
"$HADOOP_HOME/bin/hdfs" --daemon start secondarynamenode
"$HADOOP_HOME/bin/yarn" --daemon start resourcemanager
"$HADOOP_HOME/bin/yarn" --daemon start nodemanager

echo "=== [7/7] Verifying Cluster Daemons (jps) ==="
sleep 2
jps

echo ""
echo "======================================================================"
echo " Apache Hadoop Single-Node Setup Completed Successfully!"
echo " Daemons: NameNode, DataNode, SecondaryNameNode, ResourceManager, NodeManager"
echo " Web UIs:"
echo "   - HDFS NameNode:         http://localhost:9870"
echo "   - YARN Resource Manager: http://localhost:8088"
echo "======================================================================"
