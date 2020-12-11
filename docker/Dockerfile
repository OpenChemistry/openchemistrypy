# Note: This image is really only used to allow us to use webhooks to
# triggered the rebuild of dependent containers.
FROM python:3.6-slim

RUN apt-get update -y && \
  apt-get install -y git && \
  rm -rf /var/lib/apt/lists/*

COPY ./ /openchemistrypy

RUN cd /openchemistrypy && \
  pip install .
