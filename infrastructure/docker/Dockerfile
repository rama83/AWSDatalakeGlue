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
    awswrangler \
    boto3 \
    jupyter \
    jupyterlab

# Create workspace directory
WORKDIR /home/glue_user/workspace

# Add Jupyter configuration
RUN mkdir -p /home/glue_user/.jupyter
COPY infrastructure/docker/jupyter_notebook_config.py /home/glue_user/.jupyter/

# Add entrypoint script
COPY infrastructure/docker/entrypoint.sh /home/glue_user/
RUN chmod +x /home/glue_user/entrypoint.sh

# Set up entrypoint
ENTRYPOINT ["/home/glue_user/entrypoint.sh"]
