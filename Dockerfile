FROM python:3.11-slim

RUN pip install kafka-python snowflake-connector-python python-dotenv pandas snowflake-connector-python[pandas]

WORKDIR /app
COPY . /app

CMD ["bash"]