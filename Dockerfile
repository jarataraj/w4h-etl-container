# Modified from: https://pythonspeed.com/articles/activate-conda-dockerfile/
# used the `conda run` version because `conda run` appears to no longer be experimental
FROM continuumio/miniconda3:latest

# Allow statements and log messages to immediately appear in the Cloud Run logs
# SOURCE: [https://github.com/GoogleCloudPlatform/python-docs-samples/blob/main/run/logging-manual/Dockerfile]
ENV PYTHONUNBUFFERED True

# Set working directory for the project
WORKDIR /app

# Create the environment:
COPY environment.yml .
RUN conda env create -f environment.yml

# Make RUN commands use the new environment:
SHELL ["conda", "run", "-n", "w4h", "/bin/bash", "-c"]

# Set path for cartopy downloads
ENV CARTOPY_DATA_DIR=/app/cartopy_downloads
# Download shapefiles
RUN python /opt/conda/envs/w4h/bin/cartopy_feature_download.py -o /app/cartopy_downloads physical cultural cultural-extra

# Copy program files
COPY main.py retry.py utils.py near_land_complete_globe.nc ./
COPY thermofeel_fork/ ./thermofeel_fork/

# The code to run when container is started:
ENTRYPOINT ["conda", "run", "--no-capture-output", "-n", "w4h", "python", "main.py"]