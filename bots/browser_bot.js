/*
 * 매크로 봇 3단계 — 진짜 크롬을 조종해 화면에서 좌석을 누른다  [BOTS_V1]
 *
 * 사람처럼 로그인 → 좌석 페이지 → 좌석 클릭 → 예약하기 를 그대로 밟으므로
 * Referer/Origin 같은 헤더도 진짜 브라우저와 같다.
 *   STEALTH=0 : 그냥 Puppeteer. User-Agent 에 HeadlessChrome 이 남아 v0 규칙에 걸린다.
 *   STEALTH=1 : puppeteer-extra-plugin-stealth (Iliou 2021 의 고급 봇과 같은 도구).
 *               자동화 흔적을 지워 v0 를 통과한다. 남는 건 '언제, 어떻게 눌렀나'뿐이다.
 *
 * 실행 (bots/Dockerfile 이미지):
 *   docker run --rm --network <시험망> -e BASE=http://pop:8000 -e AT=21:00:00.000 \
 *     -e EMAIL=bot02@doon.kr -e STEALTH=1 doon-browser-bot
 *
 * 격리된 시험 환경이나, 사람과 합의한 수집 시간의 운영 판교(rounds.py)에만 쏜다.
 */
const puppeteer = require('puppeteer-extra');
const { BASE, AT, EMAIL, PASSWORD = 'doon1234', SEAT, STEALTH = '1', PAY = '1', XFF } = process.env;
if (STEALTH === '1') puppeteer.use(require('puppeteer-extra-plugin-stealth')());

function msUntil(hms) {           // 오늘 hh:mm:ss.fff 까지 남은 ms
  const [h, m, s] = hms.split(':');
  const t = new Date(); t.setHours(+h, +m, 0, 0);
  return t.getTime() + parseFloat(s) * 1000 - Date.now();
}

(async () => {
  if (!BASE) throw new Error('BASE 가 필요하다');
  const browser = await puppeteer.launch({ headless: true, args: ['--no-sandbox'] });
  const page = await browser.newPage();
  // 프록시 풀 흉내: 봇마다 다른 IP 로 보이게 한다 (실험판 nginx 만 이 헤더를 믿는다)  [MACRO3_V1]
  if (XFF) await page.setExtraHTTPHeaders({ 'X-Forwarded-For': XFF });

  const out = o => console.log(JSON.stringify({
    bot: STEALTH === '1' ? 'browser-stealth' : 'browser-plain', account: EMAIL || null, ...o }));

  // 1차 필터에 걸리면 좌석 요청 전에 페이지부터 막힌다. 그것도 결과로 남긴다.
  const first = await page.goto(`${BASE}/${EMAIL ? 'login.html?returnTo=/seat.html' : 'seat.html'}`);
  if (first.status() >= 400) {
    out({ stage: 'page', status: first.status(), reply: (await first.text()).slice(0, 200),
          user_agent: await browser.userAgent() });
    return browser.close();
  }

  if (EMAIL) {
    await page.type('#email', EMAIL);
    await page.type('#pw', PASSWORD);
    await Promise.all([page.waitForNavigation(), page.click('#loginBtn')]);
  }

  await page.waitForSelector('.mini-seat.available');
  const seatId = SEAT || await page.$$eval('.mini-seat.available',   // 남은 좌석 중 무작위
    els => els[Math.floor(Math.random() * els.length)].dataset.seatId);
  const sel = `.mini-seat[data-seat-id="${seatId}"]`;

  if (AT) {                       // 마지막 50ms 는 바쁜 대기로 맞춘다
    const left = msUntil(AT);
    if (left > 50) await new Promise(r => setTimeout(r, left - 50));
    await page.evaluate(end => { while (Date.now() < end); }, Date.now() + Math.max(0, msUntil(AT)));
  }

  const firedAt = new Date().toISOString();
  const reply = page.waitForResponse(r => r.url().endsWith('/seat-request'));
  const seat = await page.$eval(sel, el => (el.click(), el.dataset.seatId));
  await page.click('#reserveBtn');
  const res = await reply;
  const text = await res.text();
  const ua = await page.evaluate(() => navigator.userAgent);  // 서버가 본 값
  const accountId = await page.evaluate(() => localStorage.getItem('doon_account_id'));
  const deviceFp = await page.evaluate(() => localStorage.getItem('doon_device_fp'));

  // 이겼으면 결제한다 (매크로 업자는 표를 팔아야 하니 결제한다).  [MACRO3_V1]
  let rid = null, won = null, paid = null;
  try { rid = JSON.parse(text).request_id || null; } catch (e) {}
  if (rid) {
    won = await page.evaluate(async (s, r) => {
      for (let i = 0; i < 40; i++) {
        await new Promise(ok => setTimeout(ok, 300));
        const d = await (await fetch('/seat-status/' + s)).json();
        if (d.status === 'confirmed') return d.winner_request_id === r;
      }
      return null;
    }, seat, rid);
    if (won && PAY !== '0') {
      paid = await page.evaluate(async (s, r) => {
        const t = localStorage.getItem('doon_token');
        const h = { 'Content-Type': 'application/json' };
        if (t) h.Authorization = 'Bearer ' + t;
        return (await fetch('/payment', { method: 'POST', headers: h,
                                          body: JSON.stringify({ seat_id: s, request_id: r }) })).status;
      }, seat, rid);
    }
  }
  out({ stage: 'seat', seat, account_id: accountId || null, device_fp: deviceFp, fired_at: firedAt,
        status: res.status(), reply: text.slice(0, 200), user_agent: ua,
        requests: [{ seat, status: res.status(), request_id: rid, won, paid }] });
  await browser.close();
})().catch(e => { console.error(e); process.exit(1); });
