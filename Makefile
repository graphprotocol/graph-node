build-dev:
	cp docker/docker-compose.override.yml.dev docker/docker-compose.override.yml
	cd docker/ && sh ./setup.sh

build-stg:
	cp docker/docker-compose.override.yml.stg docker/docker-compose.override.yml
	cd docker/ && sh ./setup.sh

build-prod:
	cp docker/docker-compose.override.yml.prod docker/docker-compose.override.yml
	cd docker/ && sh ./setup.sh

run:
	cd docker/ && docker-compose up


