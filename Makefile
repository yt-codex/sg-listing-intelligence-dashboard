SOURCE_DB ?= ../propertyguru-sg-weekly-snapshots-prod-20260403-125756/data/state/propertyguru.sqlite
ANALYTICS_DB ?= data/analytics/listing_intel.sqlite
PYTHON ?= python
STREAMLIT_PORT ?= 8509

.PHONY: install test lint check refresh dashboard

install:
	$(PYTHON) -m pip install -e '.[dev]'

test:
	pytest

lint:
	ruff check .

check: test lint

refresh:
	$(PYTHON) -m sg_listing_intel.etl --source "$(SOURCE_DB)" --output "$(ANALYTICS_DB)"

dashboard:
	streamlit run app/streamlit_app.py --server.port $(STREAMLIT_PORT) -- --db "$(ANALYTICS_DB)"
