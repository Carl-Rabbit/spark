#!/usr/bin/zsh
cd ..
./build/mvn -pl :spark-catalyst_2.12 -DskipTests clean install
./build/mvn -pl :spark-sql_2.12 -DskipTests clean package
cp ~/Documents/Code/Research/qotrace/spark-qotrace/sql/catalyst/target/spark-catalyst_2.12-3.3.0.jar ~/Applications/spark-3.3.0/jars/
cp ~/Documents/Code/Research/qotrace/spark-qotrace/sql/core/target/spark-sql_2.12-3.3.0.jar ~/Applications/spark-3.3.0/jars/
echo "Copy spark jar finished"