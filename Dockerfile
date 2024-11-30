FROM python:3.12

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Install dependencies
RUN curl -sSL https://install.python-poetry.org | python3 -
RUN /root/.local/bin/poetry config virtualenvs.create false

COPY ./pyproject.toml /app/pyproject.toml
COPY ./poetry.lock /app/poetry.lock

RUN /root/.local/bin/poetry install --with prod

# Install PostgreSQL
RUN apt-get update \
  # dependencies for building Python packages
  && apt-get install -y build-essential \
  # psycopg2 dependencies
  && apt-get install -y libpq-dev

# Copy the rest of the app
COPY ./ /app

# Setup environment
ARG ENV
ARG DATABASE_URL

RUN echo "ENV=\"$ENV\"\n" >> .env
RUN echo "DATABASE_URL=\"$DATABASE_URL\"\n" >> .env

# Run the app
CMD alembic upgrade head && fastapi run app/main.py --port 80 --proxy-headers