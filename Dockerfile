FROM python:3.10-slim-bullseye

ENV PYTHONUNBUFFERED True
ENV APP_HOME /app
WORKDIR $APP_HOME
COPY . ./

RUN mkdir -p /app/public/
COPY /app/web/dbdb/build /app/public/

RUN pip install -r requirements.txt

CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8080", "--log-config", "logging.conf.yml"]
