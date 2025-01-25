docker run -d --name gold-postgres -p 5433:5432 -e POSTGRES_PASSWORD=postgres postgres


Server: localhost:5433
Username: postgres
Password: postgres
Database: postgres

docker exec -it airflow-postgres-1 psql -U airflow -d airflow
