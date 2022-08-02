
GIT_VER := $(shell git describe --tags)

image: dist/
	docker build \
		--tag ghcr.io/mashiike/psql-front:$(GIT_VER) \
		--tag ghcr.io/mashiike/psql-front:latest \
		--build-arg Version=$(GIT_VER) \
		.

release-image: image
	docker push ghcr.io/mashiike/psql-front:$(GIT_VER)
	docker push ghcr.io/mashiike/psql-front:latest
