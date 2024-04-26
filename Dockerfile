# Use Docker-in-Docker official image
FROM docker:dind

# Install Python and other dependencies
RUN apk add --no-cache python3 py3-pip \
    && ln -sf python3 /usr/bin/python

# Ensure pip is updated
RUN python -m ensurepip \
    && pip install --no-cache --upgrade pip setuptools

# Set up work directory
WORKDIR /app

# Copy the requirements file and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY . .

# Set the command to run your application
CMD ["python", "main.py"]
