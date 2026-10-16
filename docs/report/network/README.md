# 네트워크 구성도

- `doon_network.png` — 보고서용 (3520×1800)
- `doon_network.svg` — 원본 벡터
- `make_svg.py` — 구성도 좌표·글자를 정하는 스크립트 (아이콘: `icons/`, mingrammer/diagrams 프로젝트의 로고)

다시 만들기 (판교에서, 이 폴더 기준):

```bash
python3 make_svg.py    # doon_network.svg, doon_network.html
docker run --rm --user $(id -u):$(id -g) -e HOME=/tmp -e NODE_PATH=/home/pptruser/node_modules \
  -e PUPPETEER_CACHE_DIR=/home/pptruser/.cache/puppeteer -v "$PWD":/work \
  -v /usr/share/fonts/opentype/noto:/usr/share/fonts/noto:ro ghcr.io/puppeteer/puppeteer:latest node /work/shot.js
```

연결선에는 KOREN 망 여부를 적지 않았다(미확인).
