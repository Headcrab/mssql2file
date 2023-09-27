<<<<<<< HEAD
-include app.ver
export

VERSION := $(shell update_env -f app.ver -p PROJECT_VERSION) 
MAJOR := $(shell echo $(VERSION) | cut -d. -f1)
MINOR := $(shell echo $(VERSION) | cut -d. -f2)
BUILD := $(shell echo $(VERSION) | cut -d. -f3)
NEW_BUILD := $(shell echo $$(($(BUILD) + 1)))
NEW_VERSION := $(MAJOR).$(MINOR).$(NEW_BUILD)

run:
	go run $(PROJECT_PATH)

build: update_version icon
ifeq ($(OS),Windows_NT)
	GOOS=windows CGO_ENABLED=0 go build -ldflags "-s -w -X main.Name=$(PROJECT_NAME) -X main.Version=$(NEW_VERSION)" -trimpath -o ./bin/$(PROJECT_NAME).exe $(PROJECT_PATH).go
else
	@GOOS=linux CGO_ENABLED=0 go build -ldflags "-s -w -X main.Name=$(PROJECT_NAME) -X main.Version=$(NEW_VERSION)" -trimpath -o ./bin/$(PROJECT_NAME) $(PROJECT_PATH).go
endif

icon:
	rsrc -ico $(PROJECT_NAME).ico  -o $(PROJECT_PATH).syso

upx: build
ifeq ($(OS),Windows_NT)
	@upx.exe ./bin/$(PROJECT_NAME).exe 
else
	@upx ./bin/$(PROJECT_NAME)
endif

update_version:
	@update_env -f app.ver -p PROJECT_VERSION  -v $(NEW_VERSION)
	@echo $(PROJECT_NAME) v:$(NEW_VERSION)

test:
	go test ./...

lint:
	golangci-lint run

docker: update_version
	@docker build \
	--network=host \
	--build-arg PROJECT_NAME=${PROJECT_NAME} \
	--build-arg PROJECT_VERSION=${NEW_VERSION} \
	--build-arg PORT=${PORT} \
	--build-arg GOOGLE_CLIENT_ID=${GOOGLE_CLIENT_ID} \
	--build-arg GOOGLE_CLIENT_SECRET=${GOOGLE_CLIENT_SECRET} \
	-t $(PROJECT_NAME_LOW):${NEW_VERSION} .

deploy: docker undeploy
ifeq ($(OS),Windows_NT)
	@docker run -d \
	--name $(PROJECT_NAME_LOW) \
	--restart=always \
	-v x:/docker/configs/$(PROJECT_NAME):/bin/$(PROJECT_NAME)/config \
	-v x:/docker/logs/$(PROJECT_NAME):/bin/$(PROJECT_NAME)/log \
	-p $(PORT):$(PORT) \
	--add-host=host.docker.internal:host-gateway \
	$(PROJECT_NAME_LOW):${NEW_VERSION}
else
	@docker run -d \
	--name $(PROJECT_NAME_LOW) \
	--restart=always \
	-v /media/alexandr/data/work/docker/configs/$(PROJECT_NAME):/bin/$(PROJECT_NAME)/config \
	-v /media/alexandr/data/work/docker/logs/$(PROJECT_NAME):/bin/$(PROJECT_NAME)/log \
	-p $(PORT):$(PORT) \
	--add-host=host.docker.internal:host-gateway \
	$(PROJECT_NAME_LOW):${NEW_VERSION}
endif

undeploy:
	@docker rm -f $(PROJECT_NAME_LOW)
=======
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
>>>>>>> 9fb95e0 (+makefile)
