#!/usr/bin/env bash
# ==============================================================================
# Setup Apache Hadoop (Single-Node Pseudo-Distributed) on Ubuntu (WSL2)
# Course: CSE3712 — Big Data Analytics
# ==============================================================================
set -e

echo "=== [1/7] Updating apt repositories and installing Java OpenJDK 11 ==="
sudo apt-get update -y
sudo apt-get install -y openjdk-11-jdk ssh pdsh curl rsync

# Export JAVA_HOME
export JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
if ! grep -q "JAVA_HOME" ~/.bashrc; then
    echo "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64" >> ~/.bashrc
    echo "export PATH=\$PATH:\$JAVA_HOME/bin" >> ~/.bashrc
fi

echo "=== [2/7] Configuring Passwordless SSH ==="
if [ ! -f ~/.ssh/id_rsa ]; then
    ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
fi
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys
ssh-keyscan -H localhost >> ~/.ssh/known_hosts 2>/dev/null || true
ssh-keyscan -H 0.0.0.0 >> ~/.ssh/known_hosts 2>/dev/null || true

echo "=== [3/7] Downloading Apache Hadoop 3.3.6 ==="
HADOOP_DIR="$HOME/hadoop-3.3.6"
if [ ! -d "$HADOOP_DIR" ]; then
    cd "$HOME"
    if [ ! -f "hadoop-3.3.6.tar.gz" ]; then
        curl -O https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz || \
        curl -O https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
    fi
    tar -xzf hadoop-3.3.6.tar.gz
fi

export HADOOP_HOME="$HADOOP_DIR"
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"

if ! grep -q "HADOOP_HOME" ~/.bashrc; then
    echo "export HADOOP_HOME=$HADOOP_DIR" >> ~/.bashrc
    echo "export HADOOP_CONF_DIR=\$HADOOP_HOME/etc/hadoop" >> ~/.bashrc
    echo "export PATH=\$PATH:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin" >> ~/.bashrc
fi

echo "=== [4/7] Configuring Hadoop Environment and XML Files ==="
# hadoop-env.sh
sed -i '/export JAVA_HOME/d' "$HADOOP_CONF_DIR/hadoop-env.sh"
echo "export JAVA_HOME=$JAVA_HOME" >> "$HADOOP_CONF_DIR/hadoop-env.sh"

# core-site.xml
cat << 'EOF' > "$HADOOP_CONF_DIR/core-site.xml"
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
cat << 'EOF' > "$HADOOP_CONF_DIR/hdfs-site.xml"
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>file:///tmp/hadoop-hdfs/dfs/name</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>file:///tmp/hadoop-hdfs/dfs/data</value>
    </property>
</configuration>
EOF

# mapred-site.xml
cat << 'EOF' > "$HADOOP_CONF_DIR/mapred-site.xml"
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
    <property>
        <name>yarn.app.mapreduce.am.env</name>
        <value>HADOOP_MAPRED_HOME=$HADOOP_HOME</value>
    </property>
    <property>
        <name>mapreduce.map.env</name>
        <value>HADOOP_MAPRED_HOME=$HADOOP_HOME</value>
    </property>
    <property>
        <name>mapreduce.reduce.env</name>
        <value>HADOOP_MAPRED_HOME=$HADOOP_HOME</value>
    </property>
</configuration>
EOF

# yarn-site.xml
cat << 'EOF' > "$HADOOP_CONF_DIR/yarn-site.xml"
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
</configuration>
EOF

echo "=== [5/7] Formatting HDFS NameNode ==="
if [ ! -d "/tmp/hadoop-hdfs/dfs/name" ]; then
    "$HADOOP_HOME/bin/hdfs" namenode -format -force
fi

echo "=== [6/7] Starting HDFS & YARN Daemons ==="
sudo service ssh start || true
"$HADOOP_HOME/sbin/start-dfs.sh"
"$HADOOP_HOME/sbin/start-yarn.sh"

echo "=== [7/7] Verifying Cluster Daemons (jps) ==="
jps

echo ""
echo "======================================================================"
echo " Apache Hadoop Single-Node Setup Completed Successfully!"
echo " Web UIs:"
echo "   - HDFS NameNode: http://localhost:9870"
echo "   - YARN Resource Manager: http://localhost:8088"
echo "======================================================================"
