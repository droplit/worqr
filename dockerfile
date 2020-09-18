FROM node:lts

# Copy the application files
WORKDIR /usr/src/app

COPY . /usr/src/app/

# Download the required packages
RUN yarn

# Build
RUN yarn build

# Set required environment variables
ENV NODE_ENV production

# Make the application run when running the container
CMD ["yarn", "test"]