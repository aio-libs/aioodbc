# Some simple testing tasks (sorry, UNIX only).

FLAGS=


checkrst:
	python setup.py check --restructuredtext

flake: checkrst
	flake8 aioodbc tests examples setup.py

test: flake
	py.test -s $(FLAGS) ./tests/

vtest:
	py.test -s -v $(FLAGS) ./tests/

cov cover coverage: flake
	py.test -s -v  --cov-report term --cov-report html --cov aioodbc ./tests
	@echo "open file://`pwd`/htmlcov/index.html"
clean:
	rm -rf `find . -name __pycache__`
	rm -f `find . -type f -name '*.py[co]' `
	rm -f `find . -type f -name '*~' `
	rm -f `find . -type f -name '.*~' `
	rm -f `find . -type f -name '@*' `
	rm -f `find . -type f -name '#*#' `
	rm -f `find . -type f -name '*.orig' `
	rm -f `find . -type f -name '*.rej' `
	rm -f .coverage
	rm -rf coverage
	rm -rf build
	rm -rf htmlcov
	rm -rf dist

doc:
	make -C docs html
	@echo "open file://`pwd`/docs/_build/html/index.html"

docker_build:
	make -C ci build

# NOTE: we start crashing if running tests with -n auto

docker_test:
	docker run --rm -v /$$(pwd):/aioodbc -v /var/run/docker.sock:/var/run/docker.sock --name aioodbc-test-$$(date +%s) --net=host -e PYTHONASYNCIODEBUG=$(PYTHONASYNCIODEBUG) -it jettify/aioodbc-test:latest py.test -sv tests $(FLAGS)

docker_cov:
	docker run --rm -v /$$(pwd):/aioodbc -v /var/run/docker.sock:/var/run/docker.sock --name aioodbc-test-$$(date +%s) --net=host -e PYTHONASYNCIODEBUG=$(PYTHONASYNCIODEBUG) -it jettify/aioodbc-test:latest py.test -sv --cov-report term --cov-report html --cov tests --cov aioodbc $(FLAGS)

docker_clean:
	docker rm -v -f $$(docker ps -a -q -f 'name=aioodbc')

.PHONY: all flake test vtest cov clean doc
