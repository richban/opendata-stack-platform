# Use a lightweight base image, Alpine is a good choice
FROM alpine:latest

# Install curl and jq using Alpine's package manager (apk)
# The --no-cache flag reduces image size by not caching index files
RUN apk update && apk add --no-cache curl jq

# Set the entrypoint if you want a default command to run when the container starts
# For your use case in docker-compose, you might not need a specific ENTRYPOINT here,
# as you are overriding it with 'entrypoint: /bin/sh' and 'command: ...'
# ENTRYPOINT ["/bin/sh"]

# You can define a default command if no command is provided when running the container
# CMD ["/polaris-config/create-catalog.sh"]

