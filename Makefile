PROJECT_NAME=mssql2file
MODULE_NAME=mssql2file

.DEFAULT_GOAL := build

build:
ifeq ($(OS),Windows_NT)
	@go build -ldflags "-s" -o bin/$(PROJECT_NAME).exe ./cmd
	@x:\tools\upx.exe bin/$(PROJECT_NAME).exe
else
	@go build -ldflags "-s" -o bin/$(PROJECT_NAME) ./cmd
endif

run: build
ifeq ($(OS),Windows_NT)
	@bin\$(PROJECT_NAME).exe
else
	@bin/$(PROJECT_NAME)
endif

fmt:
	@go fmt ./...

test:
	@go test -v -coverprofile coverage.out ./...

coverage:
	@go tool cover -html=coverage.out

get:
	@go mod download

docker:
	@docker build -f ./build/package/Dockerfile -t $(PROJECT_NAME):latest .

deploy:
	@docker rm -f $(PROJECT_NAME)
ifeq ($(OS),Windows_NT)
	@docker run -d --name $(PROJECT_NAME) --restart=always -v x:/configs/$(PROJECT_NAME):/bin/configs -v x:/logs/$(PROJECT_NAME):/bin/logs -p 8008:8008 $(PROJECT_NAME):latest
else
	@docker run -d --name $(PROJECT_NAME) --restart=always -v /media/alexandr/data/work/configs/$(PROJECT_NAME):/bin/configs -v /media/alexandr/data/work/logs/$(PROJECT_NAME):/bin/logs -p 8008:8008 $(PROJECT_NAME):latest
endif

undeploy:
	@docker rm -f $(PROJECT_NAME)