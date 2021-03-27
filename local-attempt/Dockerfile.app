FROM python:3.8.8-slim

WORKDIR /app

# install python packages
COPY requirements.txt .
RUN python -m pip install -r requirements.txt
