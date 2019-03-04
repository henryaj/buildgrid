##
# BuildGrid's Docker build manifest.
#
#  ¡FOR LOCAL DEVELOPMENT ONLY!
#
# Builds an image from local sources.
#

FROM debian:buster

RUN  [ \
"apt-get", "update" ]
RUN  [ \
"apt-get", "install", "-y", \
"python3", "python3-venv", "python3-pip", \
"bubblewrap", "fuse3" \
]

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

# Install the test requirements:
ADD requirements.tests.txt /app
RUN [ \
"/app/env/bin/python", "-m", "pip", \
"install", "--requirement", \
"requirements.tests.txt" \
]

# Copy the repo. contents:
COPY . /app

# Add tools directory to the PATH:
ENV PATH=$PATH:/app/tools

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
