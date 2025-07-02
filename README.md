# Apache Airflow on Docker

This project sets up Apache Airflow 2.8.1 using Docker Compose with a PostgreSQL backend and LocalExecutor.

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
