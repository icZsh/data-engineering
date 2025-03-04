FROM python:3.9.1

RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app
COPY upload_data.py upload_data.py

ENTRYPOINT [ "python", "upload_data.py" ]