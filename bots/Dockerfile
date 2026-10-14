#  docker build -t doon-browser-bot bots/
FROM ghcr.io/puppeteer/puppeteer:latest
RUN npm i puppeteer-extra puppeteer-extra-plugin-stealth
# AT(발사 시각)을 한국 시각으로 읽는다. 없으면 UTC 로 읽어 9시간 기다린다.
ENV TZ=Asia/Seoul
# 한국에서 돌리는 매크로처럼 크롬 언어를 한국어로 (Accept-Language: ko-KR,ko;q=0.9).
# 기본값 en-US 로 두면 AI 가 '영어 브라우저 = 봇' 이라는 가짜 차이를 배운다.
ENV LANG=ko_KR.UTF-8 LANGUAGE=ko_KR:ko
COPY browser_bot.js .
CMD ["node", "browser_bot.js"]
