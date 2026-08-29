FROM apache/airflow:3.3.0

# Копируем список зависимостей внутрь контейнера
COPY requirements.txt /requirements.txt

# Устанавливаем библиотеки от пользователя airflow
USER airflow
RUN pip install --no-cache-dir -r /requirements.txt