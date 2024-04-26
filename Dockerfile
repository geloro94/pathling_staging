# Use Docker-in-Docker official image
FROM docker:dind

# Install Python and pip via apk
RUN apk add --no-cache python3 py3-pip \
    && ln -sf python3 /usr/bin/python

# Set up a virtual environment in the container
RUN python3 -m venv /venv
ENV PATH="/venv/bin:$PATH"

# Set up work directory
WORKDIR /app

# Copy the requirements file
COPY requirements.txt .

# Install Python dependencies within the virtual environment using pip
RUN pip install --no-cache --upgrade pip setuptools \
    && pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY . .

# Set the command to run your application
CMD ["python", "main.py"]
