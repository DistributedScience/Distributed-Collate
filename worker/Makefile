user = distributedscience
project = distributed-collate
org = distributedscience
tag = latest

.DEFAULT_GOAL: build
build:
	docker build -t $(org)/$(project):$(tag) . 
	docker login -u $(user)
	docker push $(org)/$(project):$(tag)
	echo "Your new docker registry is ready at: $(org)/$(project):$(tag)"

