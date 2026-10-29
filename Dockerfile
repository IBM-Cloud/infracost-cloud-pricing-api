# Dockerfile built from https://github.ibm.com/dreadnought/dreadnought-common-images nodejs22 image

# Building locally requires pointing to an `.npmrc` file to pull from Dreadnought's Artifactory instance, you can create one locally or point to your higher level file.

# Before building locally make sure to log into ibmcloud cli and container registry by following:

  # ibmcloud login --sso <select correct account number for dn-dev-d-appgalley>
  # ibmcloud cr login

# To build locally run `docker build . -t <tag> --secret id=npmrc,src=<.npmrc path>` from this repo's root

# To run locally with browser access to it run `docker run --env-file <.env path> -itp 8081:8081 <image ID>`,
# you can change `-it` to `-d` if you wish to run in detached mode instead of interactive.

# The TAG ARG lets you pass in the latest version of the image
ARG TAG

# First stage is the nodejs20-builder image
FROM !NODE_IMAGE_NAME! as nodebuild

# Disable NPM version notices.
ENV NPM_CONFIG_UPDATE_NOTIFIER=false

# Change the workdir to match your module
WORKDIR /usr/src/app

# Ensure we're on an acceptable npm version
RUN npm -v

# Copy your files into the working directory
COPY package*.json ./
RUN npm install --production \
  && cp -R node_modules prod_node_modules \
  && npm install
COPY . .
RUN npm run build


# Copy over what we need to a new layer and provide the app entrypoint
FROM scratch
LABEL compliance.owner="ibm-cloud-cost"

USER 1000
COPY --from=nodebuild --chown=1000 /image /
COPY --from=nodebuild --chown=1000 /usr/src/app/prod_node_modules ./node_modules
COPY --from=nodebuild --chown=1000 /usr/src/app/dist ./dist
COPY --from=nodebuild --chown=1000 /usr/src/app/package*.json /usr/src/app

# RUN apk add --no-cache bash curl postgresql-client

RUN mkdir -p /usr/src/app/data/products


ENV NODE_ENV=production
EXPOSE 4000

WORKDIR /usr/src/app
CMD [ "node", "dist/server.js" ]
