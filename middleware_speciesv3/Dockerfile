FROM node:24-alpine3.23

WORKDIR /usr/src/app

COPY package*.json ./
ENV NODE_ENV=production
# RUN npm ci --omit=dev
RUN npm ci --omit=dev --verbose || (echo '--- NPM DEBUG LOG ---' && cat /root/.npm/_logs/*-debug-0.log && exit 1)

COPY . .

EXPOSE 8080
CMD ["npm", "start"]



