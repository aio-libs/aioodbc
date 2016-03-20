copy:
	mkdir -p aioodbc
	rsync -ract ../aioodbc ../requirements-dev.txt ../setup.py ../README.rst ../CHANGES.txt aioodbc/
build: copy
	docker build --rm -t jettify/aioodbc .
