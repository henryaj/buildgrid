FROM python:3.5-stretch

# Point the path to where buildgrid gets installed
ENV PATH=$PATH:/root/.local/bin/

# Upgrade python modules
RUN python3 -m pip install --upgrade setuptools pip

# Use /app as the current working directory
WORKDIR /app

# Copy the repo contents (source, config files, etc) in the WORKDIR
COPY . .

# Install BuildGrid
RUN pip install --user --editable .

# Entry Point of the image (should get an additional argument from CMD, the path to the config file)
ENTRYPOINT ["bgd", "server", "start", "-vv"]

# Default config file (used if no CMD specified when running)
CMD ["buildgrid/_app/settings/default.yml"]
