# Use the official Python image with uv pre-installed
FROM ghcr.io/astral-sh/uv:python3.10-bookworm-slim

# Set the working directory
WORKDIR /app
RUN mkdir src

# Set environment variables
ENV UV_SYSTEM_PYTHON=1
ENV PATH="/root/.local/bin:$PATH"

# Copy dependencies

COPY ./src /app/src/
COPY requirements.txt .
COPY credentials.json .

# Install dependencies
RUN uv pip install -r requirements.txt
