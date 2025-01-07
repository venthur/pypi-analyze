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

get-dataset: $(VENV)
	curl --remote-name https://raw.githubusercontent.com/pypi-data/data/main/links/dataset.txt
	$(BIN)/python3 main.py --trim-dataset dataset.txt
	curl -L -C - --remote-name-all --parallel --create-dirs --output-dir $(DATASET_DIR) $$(cat dataset.txt)
	rm -f results.parquet

get-data: $(VENV)
	$(BIN)/python3 main.py --fetch-data

analysis: $(VENV)
	$(BIN)/python3 main.py --analyze

clean:
	rm -rf $(VENV)
