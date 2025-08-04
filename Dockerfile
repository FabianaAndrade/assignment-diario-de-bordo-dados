FROM openjdk:11

RUN apt-get update && apt-get install -y python3 python3-pip

WORKDIR /app

COPY . .

RUN pip3 install -r requirements.txt

CMD ["python3", "main.py"]