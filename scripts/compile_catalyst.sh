#!/usr/bin/zsh

./build/mvn -pl :spark-catalyst_2.12 -DskipTests clean package
cp ~/workspace/yzx_workspace/spark/sql/catalyst/target/spark-catalyst_2.12-3.3.0.jar ~/env/spark-3.3.0-bin-hadoop3/jars/
echo 'Copy spark jar finished'