# How to install and run

1. You need to have `maven` installed in your system
```
sudo apt install maven
```
2. build the maven project
```
mvn package
```
3. run RDCM with a default `.config` file
```
java -cp target/rdcm-1.0-SNAPSHOT.jar com.metsr.hpc.RemoteDataClientManager ../scripts/run.config
```

