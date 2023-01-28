#!/usr/bin/zsh

./build/mvn -pl :spark-network-common_2.12 -DskipTests clean package
cp ~/workspace/yzx_workspace/spark/common/network-common/target/spark-network-common_2.12-3.3.0.jar ~/env/spark-3.3.0-bin-hadoop3/jars/
echo 'Copy spark jar finished'