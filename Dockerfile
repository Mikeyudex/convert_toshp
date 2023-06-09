# syntax=docker/dockerfile:1

FROM python:3.11-slim-buster

WORKDIR /python-docker

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY . .

EXPOSE 5500

#CMD [ "python3",  "app.py"]
#CMD [ "python3", "-m" , "flask", "run", "--host=0.0.0.0"]
CMD [ "python3", "-m" , "flask", "run", "--host=0.0.0.0", "--port=5500"]
#CMD [ "python3", "app.py"]