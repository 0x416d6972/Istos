FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir uv

COPY pyproject.toml uv.lock README.md ./
COPY src/ src/

RUN uv pip install --system -e ".[all]"

# Add your Istos service as main.py (scaffold one with `istos new`), then it runs
# on container start. The istos-service block in docker-compose.yml builds this
# image and runs `python main.py`.
# COPY main.py ./

CMD ["python", "main.py"]
