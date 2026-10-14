#  docker build -t doon-browser-bot bots/
FROM ghcr.io/puppeteer/puppeteer:latest
RUN npm i puppeteer-extra puppeteer-extra-plugin-stealth
# AT(발사 시각)을 한국 시각으로 읽는다. 없으면 UTC 로 읽어 9시간 기다린다.
ENV TZ=Asia/Seoul
COPY browser_bot.js .
CMD ["node", "browser_bot.js"]
