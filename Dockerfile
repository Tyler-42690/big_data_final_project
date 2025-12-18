FROM python:3.11-slim-buster

WORKDIR /app

COPY pyproject.toml ./

RUN uv pip install --no-cache-dir .

COPY . .

CMD [ "python", "test.py" ]