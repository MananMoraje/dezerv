# Use an official Python runtime as a parent image
FROM python:3.12-slim

# Install R and required R packages
RUN apt-get update && apt-get install -y \
    r-base \
    libcurl4-openssl-dev \
    libssl-dev \
    libxml2-dev \
    && apt-get clean

# Install necessary R packages
RUN R -e "install.packages(c('readxl', 'writexl'), repos='http://cran.rstudio.com/')"

# Make port 5000 available to the world outside this container
EXPOSE 5000

WORKDIR /app
COPY . /app

# Run app.py when the container launches
CMD ["python", "main.py"]
