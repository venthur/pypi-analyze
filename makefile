DATASET_DIR = data
DATASET_FILE := dataset.txt

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
	curl --silent -o $(DATASET_FILE) https://raw.githubusercontent.com/pypi-data/data/main/links/dataset.txt
	@if $(BIN)/python3 main.py --trim-dataset $(DATASET_FILE); then \
		echo "No changes in data set."; \
	else \
		echo "Updating data set"; \
		curl -L -C - --remote-name-all --parallel --create-dirs --output-dir $(DATASET_DIR) $$(cat $(DATASET_FILE)); \
		rm -f results.parquet; \
	fi

get-data: $(VENV)
	$(BIN)/python3 main.py --fetch-data

analysis: $(VENV)
	$(BIN)/python3 main.py --analyze

update-requirements: $(VENV)
	$(BIN)/pur

clean:
	rm -rf $(VENV)
