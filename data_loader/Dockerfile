# Start from an official Python base image.
# Using a 'slim' version is a good practice for smaller image sizes.
FROM python:3.9-slim

# Set the working directory inside the container.
# All subsequent commands will be run from this directory.
WORKDIR /app

# Copy the requirements file first. This is a Docker caching optimization.
# If the requirements don't change, this layer won't be rebuilt.
COPY requirements.txt .

# Install the Python dependencies.
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container.
COPY . .

# The command that will be executed when the container starts.
CMD ["python", "load_data.py"]
