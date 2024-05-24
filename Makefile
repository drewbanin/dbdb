
dev:
	uvicorn server:app --reload

build-web:
	cd ./web/dbdb && NODE_ENV=production npm run build

deploy:
	fly deploy -a dbdb --debug
