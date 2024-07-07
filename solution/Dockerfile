FROM python:3.9.7

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    default-libmysqlclient-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir --upgrade pip

RUN pip install pandas && python -c "import pandas" && echo "pandas installed successfully"
RUN pip install mysql-connector-python && python -c "import mysql.connector" && echo "mysql-connector-python installed successfully"
RUN pip install SQLAlchemy && python -c "import sqlalchemy" && echo "SQLAlchemy installed successfully"
RUN pip install mysqlclient && python -c "import MySQLdb" && echo "mysqlclient installed successfully"

COPY . .

CMD ["bash"]