### Upload resource to HDFS: 

```bash
hdfs dfs -copyFromLocal resources/retails.csv /

```

check your file with command: 

```bash
hdfs dfs -ls /

```

### Run 

Buil file jar with mvn: 
```
mvn clean package
```

Run project with spark-submit: 
```
spark-submit --class com.spark.part_4.Main target/Retails-V1.jar
```

(instead part_4 to part_1,2,3 or 5 if you want run part 1,2,3,5)
