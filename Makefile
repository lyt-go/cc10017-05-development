.PHONY: install backend frontend test test-backend typecheck migrate-check migrate-up migrate-down

install:
	cd backend && python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
	cd frontend && npm ci

backend:
	cd backend && ./run.sh

frontend:
	cd frontend && npm run dev

test: test-backend typecheck

test-backend:
	cd backend && .venv/bin/python -m pytest -q

typecheck:
	cd frontend && npx vue-tsc --noEmit

migrate-check:
	cd backend && ./migrations/apply.sh check

migrate-up:
	cd backend && ./migrations/apply.sh up

migrate-down:
	cd backend && ./migrations/apply.sh down
