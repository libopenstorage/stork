TORPEDO_IMG=$(DOCKER_HUB_REPO)/$(DOCKER_HUB_TORPEDO_IMAGE):$(DOCKER_HUB_TAG)

all: clean
	@go build -o src/torpedo src/torpedo.go

	@echo "Building container: docker build --tag $(TORPEDO_IMG) -f src/Dockerfile ."
	sudo docker build --tag $(TORPEDO_IMG) -f src/Dockerfile src

deploy: all
	docker push $(TORPEDO_IMG)

clean:
	-@rm -rf src/torpedo
	-@docker rmi -f $(TORPEDO_IMG)
