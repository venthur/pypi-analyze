DATASET_DIR = data

VENV = venv
BIN = $(VENV)/bin

$(VENV): requirements.txt
	python3 -m venv $(VENV)
	$(VENV)/bin/pip install --upgrade -r requirements.txt
	touch $(VENV)

.PHONY:
run: $(VENV)
	$(BIN)/python3 main.py

get-dataset:
	curl -L --remote-name-all --parallel --create-dirs --output-dir $(DATASET_DIR) $(shell curl -L "https://github.com/pypi-data/data/raw/main/links/dataset.txt")

clean:
	rm -rf $(VENV)
