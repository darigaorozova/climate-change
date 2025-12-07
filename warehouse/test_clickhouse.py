import clickhouse_connect

client = clickhouse_connect.get_client(
    host='localhost',
    port=8123,     # HTTP порт по умолчанию
    username='default',
    password=''
)

# Выполним простой запрос
result = client.query('SELECT version()')

print("ClickHouse version:", result.result_rows)
