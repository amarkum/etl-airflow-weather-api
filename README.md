## Commands to run airflow

```shell
sudo airflow db init
sudo airflow users create --role Admin --username admin --password admin --email admin@example.com --firstname foo --lastname bar
sudo airflow webserver -p 8080
sudo airflow scheduler
```

enable_xcom_pickling = True