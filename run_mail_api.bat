@echo off

echo Stopping existing container...
docker stop mail-api
docker rm mail-api

echo Building new Docker image...
docker build -t mail-api .

echo Running container...
docker run -d --name mail-api -p 8000:8000 --env-file .env mail-api


echo API Logs....
docker logs -f mail-api