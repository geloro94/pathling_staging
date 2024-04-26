FROM docker:dind

# Install Python and pip via apk
RUN apk add --no-cache python3 py3-pip \
    && ln -sf python3 /usr/bin/python

# Set up a virtual environment in the container
RUN python3 -m venv /venv
ENV PATH="/venv/bin:$PATH"

# Install dependencies
RUN pip install --no-cache --upgrade pip setuptools

# Set up work directory
WORKDIR /app

# Copy the requirements and install them
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY . .

# Generate a self-signed certificate
RUN openssl req -x509 -newkey rsa:4096 -nodes -keyout key.pem -out cert.pem -days 365 \
    -subj "/C=US/ST=State/L=City/O=Organization/CN=localhost"

# Start Docker daemon and then your application
ENTRYPOINT ["dockerd-entrypoint.sh"]
CMD ["gunicorn", "-w", "4", "-b", "0.0.0.0:8000", "--certfile", "cert.pem", "--keyfile", "key.pem", "main:app"]
