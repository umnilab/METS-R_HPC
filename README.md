# How to install and run


* You need to have `maven` installed in your system
```
sudo apt install maven
```
* build the maven project
```
cd rdcm
mvn dependency:copy-dependencies
mvn package
```
* Update the `run.config` file in `scripts` directory with your configurations and run HPC module using.
```
cd ../scripts
python run_test.py run.config
```

