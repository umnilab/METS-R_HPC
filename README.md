# How to install and run


* You need to have `maven` installed in your system
```
sudo apt install maven
```
* build the maven project
```
cd rdcm
mvn package
```
* run RDCM with a default `.config` file
```
java -cp target/rdcm-1.0-SNAPSHOT.jar com.metsr.hpc.RemoteDataClientManager ../scripts/run.config
```

