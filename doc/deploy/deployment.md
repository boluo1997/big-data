# Deploy


start the jar using windows
```shell
# mock 
java -jar data-mock-1.0-SNAPSHOT.jar {"job_env":dev,"job_type":daily}

# batch
java -jar data-batch-ingestion-1.0-SNAPSHOT.jar {"job_env":dev,"job_type":daily}

# streaming
java -jar -Dspring.profiles.active=DEV -Djob=job_trade data-streaming-ingestion-1.0-SNAPSHOT.jar 

```



start the jar using linux
```shell
# mock 
cd /opt/big-data/bin/
java -Xms470m -Xmx470m -jar data-mock-1.0-SNAPSHOT.jar '{"job_env":"dev","job_type":"daily"}'
```



```shell

# 查看当前用户的定时任务
crontab -l

# 编辑定时任务
crontab -e
15 0 * * * java -Xms470m -Xmx470m -jar /opt/big-data/bin/data-mock-1.0-SNAPSHOT.jar '{"job_env":"dev","job_type":"daily"}'

# 这个会执行失败
15 0 * * * java -Xms470m -Xmx470m -jar /opt/big-data/bin/data-mock-1.0-SNAPSHOT.jar '{"job_env":"dev","job_type":"daily"}' >> /home/user/big-data/data-mock.log 2>&1

```
