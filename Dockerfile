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
# Install Gunicorn in addition to other requirements
RUN pip install --no-cache --upgrade pip setuptools \
    && pip install --no-cache-dir -r requirements.txt \
    && pip install gunicorn

# Copy the rest of the application
COPY . .

# Command to run the Flask application with Gunicorn
# Adjust the number of workers and threads as necessary
CMD ["gunicorn", "-w", "4", "-b", "0.0.0.0:8000", "main:app"]
