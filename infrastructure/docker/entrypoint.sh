#!/bin/bash
set -e

# Start Spark History Server
mkdir -p /home/glue_user/spark/spark-events
/home/glue_user/spark/sbin/start-history-server.sh &

# Start Livy server
/home/glue_user/spark/bin/livy-server start &

# Start Jupyter Lab if requested
if [ "${JUPYTER_ENABLE_LAB}" = "yes" ]; then
    echo "Starting Jupyter Lab..."
    cd /home/glue_user/workspace
    jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root &
fi

# Print welcome message
echo "========================================================"
echo "AWS Glue 5.0 Development Environment"
echo "========================================================"
echo "Spark UI:           http://localhost:4040"
echo "Spark History:      http://localhost:18080"
echo "Jupyter Lab:        http://localhost:8888/?token=glue"
echo "MinIO Console:      http://localhost:9001"
echo "========================================================"
echo "To run a Glue job:  ./scripts/run_local_job.sh <job_name>"
echo "========================================================"

# Keep container running
if [ $# -eq 0 ]; then
    # If no command is provided, keep the container running
    tail -f /dev/null
else
    # If a command is provided, execute it
    exec "$@"
fi
