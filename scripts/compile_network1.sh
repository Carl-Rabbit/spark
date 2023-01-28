#!/usr/bin/zsh
cd ..
./build/mvn -pl :spark-network-common_2.12 -DskipTests clean package
cp ~/Documents/Code/Research/qotrace/spark-qotrace/common/network-common/target/spark-network-common_2.12-3.3.0.jar ~/Applications/spark-3.3.0/jars/
echo 'Copy spark jar finished'