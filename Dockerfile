FROM apache/airflow:2.9.0-python3.12

# Copy the uv binary from the official image for fast, reliable package management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Copy pyproject.toml to reference dependencies
COPY pyproject.toml ./

# 1. Compile the project dependencies using uv (handles Python 3.12 compatibility perfectly)
# 2. Exclude apache-airflow (already pre-installed in the base image)
# 3. Exclude torch/torch-geometric (keeps the image slim and build times fast)
# 4. Install the remaining dependencies into the container's Python environment
RUN uv pip compile pyproject.toml -o requirements.txt && \
    sed -i '/apache-airflow/d; /torch/d; /torch-geometric/d' requirements.txt && \
    uv pip install --system --no-cache -r requirements.txt
