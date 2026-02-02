sync:
	uv sync --all-extras --dev

publish-test:
	uv publish --index=testpypi --trusted-publishing=always

publish:
	uv publish --index=pypi --trusted-publishing=always

build:
	uv build -o dist/ --no-sources

lint:
	uv run ruff check --fix-only .
	uv run ruff format .

type-check:
	uv run pyrefly check

test:
	uv run pytest tests/ -v --tb=short 2>&1

test-lima:
	TESTCONTAINERS_RYUK_DISABLED=true DOCKER_HOST=unix:///Users/pyro/.lima/default/sock/docker.sock uv run pytest tests/ -v --tb=short 2>&1
