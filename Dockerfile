##
# BuildGrid's Docker build manifest.
#
#  ¡FOR LOCAL DEVELOPMENT ONLY!
#
# Builds an image from local sources.
#

FROM python:3.5-stretch

# Use /app as working directory:
WORKDIR /app

# Create a virtual environment:
RUN [ \
"python3", "-m", "venv", "/app/env" \
]

# Upgrade Python core modules:
RUN [ \
"/app/env/bin/python", "-m", "pip", \
"install", "--upgrade", \
"setuptools", "pip", "wheel" \
]

# Install the main requirements:
ADD requirements.txt /app
RUN [ \
"/app/env/bin/python", "-m", "pip", \
"install", "--requirement", \
"requirements.txt" \
]

# Install the auth. requirements:
ADD requirements.auth.txt /app
RUN [ \
"/app/env/bin/python", "-m", "pip", \
"install", "--requirement", \
"requirements.auth.txt" \
]

# Copy the repo. contents:
COPY . /app

# Install BuildGrid:
RUN [ \
"/app/env/bin/python", "-m", "pip", \
"install", "--editable", \
".[auth,tests]" \
]

# Entry-point for the image:
ENTRYPOINT [ \
"/app/env/bin/bgd" \
]

# Default command (default config.):
CMD [ \
"server", "start", \
"data/config/default.conf", \
"-vvv" \
]
