
dev:
	uvicorn server:app --reload

build-web:
	echo "building web assets"
	pushd .
	cd ./web/dbdb && NODE_ENV=production npm run build
	popd

deploy-fly:
	echo "deploying to fly.io"
	fly deploy -a dbdb --debug

deploy: build-web deploy-fly
	echo "done!"

