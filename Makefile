SOURCE_DB ?= ../propertyguru-sg-weekly-snapshots-prod-20260403-125756/data/state/propertyguru.sqlite
ANALYTICS_DB ?= data/analytics/listing_intel.sqlite
PYTHON ?= python
STREAMLIT_PORT ?= 8509

.PHONY: install test lint check refresh dashboard export-static refresh-from-snapshot serve-static

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

export-static:
	$(PYTHON) scripts/export_static_data.py --db "$(ANALYTICS_DB)" --out docs/assets

refresh-from-snapshot:
	PYTHON="$(PYTHON)" SOURCE_DB="$(SOURCE_DB)" ANALYTICS_DB="$(ANALYTICS_DB)" ./scripts/refresh_from_pg_snapshot.sh

serve-static:
	$(PYTHON) -m http.server 8513 -d docs --bind 127.0.0.1
