# Apache Airflow on Docker

This project sets up Apache Airflow 2.8.1 using Docker Compose with a PostgreSQL backend and LocalExecutor.
Data comes from MLB api (https://github.com/toddrob99/MLB-StatsAPI)
Visualization will be displayed in Streamlit public app.

## 🔧 Setup

```bash
git clone <repo>
cd airflow-docker
docker-compose up -d
```

Access the Airflow UI at [http://localhost:8080](http://localhost:8080) or `http://<EC2-IP>:8080`.

## 🧪 Default Login

- Username: `admin`
- Password: `admin`

## 📁 Project Structure

- `dags/`: Your DAG files
- `logs/`: Airflow logs
- `plugins/`: Optional custom plugins

## 🧼 Shut down

```bash
docker-compose down
```
