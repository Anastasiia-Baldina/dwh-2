### Сборка и запуск
```shell
docker-compose up -d
```
### Остановка
```shell
docker-compose down -v
```

### Заливка из staging в detailed Windows(PowerShell)
```shell
Get-Content -Raw .\sql\load_staging_to_detailed.sql | docker exec -i trino trino --file /dev/stdin
```

### Генерация DDL скриптов по схеме
```shell
pip install pyyaml
python scripts/generate_source_ddl.py
python scripts/generate_staging_ddl.py
python scripts/generate_detailed_ddl.py
```
