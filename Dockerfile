# 1. Start with an official Python base image
FROM python:3.9-slim

# Set the environment variable to signal we are in a Docker container
ENV DOCKER_ENV=true

# 2. Set the working directory inside the container
WORKDIR /app

# 3. Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. Copy your data directory and the Python script
COPY ./data ./data
COPY ./solution/solution.py .

# 5. Specify the command to run when the container starts
CMD ["python", "solution.py"]