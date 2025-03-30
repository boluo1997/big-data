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

