
dev:
	uvicorn server:app --reload

deploy:
	fly deploy -a dbdb --debug
