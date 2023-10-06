cd fixtures && docker-compose down -v && docker-compose up -d && cd ..
docker rmi $(docker images | grep 'dev-' |awk '{print $3}')
go build