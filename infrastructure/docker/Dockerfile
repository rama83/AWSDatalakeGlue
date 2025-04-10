# Use the AWS Glue 5.0 container as base
FROM amazon/aws-glue-libs:glue_libs_5.0.0_image_01

# Set environment variables
ENV PYTHONPATH=/home/glue_user/workspace
ENV GLUE_HOME=/home/glue_user

# Install additional Python packages
RUN pip install --no-cache-dir \
    pytest \
    pytest-cov \
    moto \
    black \
    isort \
    flake8 \
    mypy \
    python-dotenv \
    awswrangler

# Create workspace directory
WORKDIR /home/glue_user/workspace

# Copy project files
COPY . .

# Set up entrypoint
ENTRYPOINT ["/bin/bash"]
