# Base Node.js
FROM node:20-alpine

WORKDIR /app

# Copier package.json et installer les dépendances
COPY package.json yarn.lock ./
RUN yarn install --production

# Copier le code
COPY . .

# Lancer le worker
#CMD ["node", "src/server.js"]
