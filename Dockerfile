
FROM python:3.11

RUN mkdir /app

# where your code lives  WORKDIR $DockerHOME
WORKDIR /app

ADD . /app/
# install dependencies
RUN pip install --upgrade pip

# run this command to install all dependencies
ADD requirements.txt /app
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

EXPOSE 9000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "9000"]
