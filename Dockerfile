FROM ubuntu:16.04

RUN apt-get -y update && apt-get install -y build-essential python python-dev \
python-pip nodejs libmysqlclient-dev

RUN pip install flask appdirs

RUN useradd -m tempuser

COPY . /home/tempuser/elo-frontend

RUN chown -R tempuser:tempuser /home/tempuser/elo-frontend

WORKDIR /home/tempuser/elo-frontend/

USER tempuser

RUN python setup.py build

USER root

RUN python setup.py install

USER tempuser

CMD elo_frontend
