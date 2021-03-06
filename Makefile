WORKDIR="github.com/NeowayLabs/es-export"

IMAGENAME="neowaylabs/es-export"
REGISTRY="hub.docker.com"
IMAGE=$(REGISTRY)/$(IMAGENAME):1.0.0

all: image
	@echo "Create image: ${IMAGE}"

install: deploy

image: build
	docker build -t $(IMAGE) .

deploy: image
	docker push $(IMAGE)

build: build-env
	docker run --rm -v `pwd`:/go/src/$(WORKDIR) --privileged -i -t $(IMAGENAME) bash hack/make.sh

build-env:
	docker build -t $(IMAGENAME) -f ./hack/Dockerfile .

check: build
	docker run --rm -v `pwd`:/go/src/$(WORKDIR) --privileged -i -t $(IMAGENAME) bash hack/check.sh

shell: build
	docker run --rm -v `pwd`:/go/src/$(WORKDIR) --privileged -i -t $(IMAGENAME) bash

