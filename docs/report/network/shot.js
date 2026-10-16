const puppeteer = require('puppeteer');
(async () => {
  const b = await puppeteer.launch({ args: ['--no-sandbox'] });
  const p = await b.newPage();
  await p.setViewport({ width: 1720, height: 870, deviceScaleFactor: 2 });
  await p.goto('file:///work/doon_network.html');
  await p.evaluate(() => document.fonts.ready);
  await p.screenshot({ path: '/work/doon_network.png' });
  await b.close();
})();
