FROM python:3.10-slim-bullseye

ENV PYTHONUNBUFFERED True
ENV APP_HOME /app
WORKDIR $APP_HOME
COPY . ./

RUN cd ./web/dbdb && npm install
RUN cd ./web/dbdb && npm run build

RUN mkdir -p public/
COPY ./web/dbdb/build public/

RUN pip install -r requirements.txt

CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8080"]
