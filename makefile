DATASET_DIR = data

VENV = venv
BIN = $(VENV)/bin

DOWNLOADED_FILE := /tmp/new_dataset.txt
EXISTING_FILE := dataset.txt

$(VENV): requirements.txt
	python3 -m venv $(VENV)
	$(VENV)/bin/pip install --upgrade -r requirements.txt
	touch $(VENV)

.PHONY:
run: $(VENV)
	$(BIN)/python3 main.py

get-dataset: $(VENV)
	curl -o $(DOWNLOADED_FILE) https://raw.githubusercontent.com/pypi-data/data/main/links/dataset.txt
	@if cmp -s $(DOWNLOADED_FILE) $(EXISTING_FILE); then \
		echo "No changes in $(EXISTING_FILE)"; \
	else \
		echo "Updating $(EXISTING_FILE)"; \
		mv $(DOWNLOADED_FILE) $(EXISTING_FILE); \
		$(BIN)/python3 main.py --trim-dataset $(EXISTING_FILE); \
		curl -L -C - --remote-name-all --parallel --create-dirs --output-dir $(DATASET_DIR) $$(cat $(EXISTING_FILE)); \
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
