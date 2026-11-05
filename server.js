/**
 * Green Net রিমোট সাহায্যের রিলে সার্ভার।
 *
 * গ্রাহকের অ্যাপ /app এ যুক্ত হয়ে ৬ সংখ্যার কোড পায়। টেকনিশিয়ান "/" পেজে পাসওয়ার্ড ও কোড দিয়ে
 * /tech এ যুক্ত হয়। সার্ভার শুধু দুই পক্ষের মধ্যে বার্তা পৌঁছে দেয়, কিছু জমা রাখে না।
 *
 * পরিবেশ চলক (Environment variables):
 *   TECH_PASSWORD  টেকনিশিয়ানের পাসওয়ার্ড (বাধ্যতামূলক)
 *   PORT           পোর্ট (Render নিজে দেয়)
 */
const http = require('http');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = Number(process.env.PORT) || 3000;
const TECH_PASSWORD = process.env.TECH_PASSWORD || '';
const CODE_TTL_MS = 10 * 60 * 1000;      // কোড ১০ মিনিট চলে
const SESSION_MAX_MS = 30 * 60 * 1000;   // সেশন সর্বোচ্চ ৩০ মিনিট
const MAX_FAILS = 5;                      // এতবার ভুল পাসওয়ার্ড বা কোড হলে
const LOCK_MS = 10 * 60 * 1000;           // এতক্ষণ বন্ধ

/** code -> { app, tech, codeTimer, maxTimer } */
const sessions = new Map();
/** ip -> { n, until } */
const fails = new Map();

const sha = (s) => crypto.createHash('sha256').update(String(s)).digest();
const passwordOk = (given) =>
  TECH_PASSWORD.length > 0 && crypto.timingSafeEqual(sha(given), sha(TECH_PASSWORD));

function newCode() {
  for (let i = 0; i < 50; i++) {
    const c = String(crypto.randomInt(0, 1000000)).padStart(6, '0');
    if (!sessions.has(c)) return c;
  }
  return null;
}

function send(ws, obj) {
  if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj));
}

function endSession(code, reason) {
  const s = sessions.get(code);
  if (!s) return;
  sessions.delete(code);
  clearTimeout(s.codeTimer);
  clearTimeout(s.maxTimer);
  for (const w of [s.app, s.tech]) {
    if (w && w.readyState === 1) {
      send(w, { t: 'ended', reason });
      w.close(1000, reason);
    }
  }
}

function ipOf(req) {
  const xf = req.headers['x-forwarded-for'];
  return (xf ? String(xf).split(',')[0].trim() : req.socket.remoteAddress) || 'unknown';
}

function locked(ip) {
  const f = fails.get(ip);
  return f && f.until > Date.now();
}

function addFail(ip) {
  const f = fails.get(ip) || { n: 0, until: 0 };
  f.n += 1;
  if (f.n >= MAX_FAILS) {
    f.n = 0;
    f.until = Date.now() + LOCK_MS;
  }
  fails.set(ip, f);
}

const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'content-type': 'text/plain' });
    return res.end('ok');
  }
  if (req.url === '/' || req.url.startsWith('/?')) {
    res.writeHead(200, {
      'content-type': 'text/html; charset=utf-8',
      'cache-control': 'no-store',
    });
    return res.end(PANEL_HTML);
  }
  res.writeHead(404);
  res.end('not found');
});

const wss = new WebSocketServer({ noServer: true, maxPayload: 4 * 1024 * 1024 });

server.on('upgrade', (req, socket, head) => {
  const path = (req.url || '').split('?')[0];
  if (path !== '/app' && path !== '/tech') {
    socket.destroy();
    return;
  }
  wss.handleUpgrade(req, socket, head, (ws) => {
    ws.isAlive = true;
    ws.on('pong', () => (ws.isAlive = true));
    ws.ip = ipOf(req);
    if (path === '/app') onApp(ws);
    else onTech(ws);
  });
});

// ---------- গ্রাহকের অ্যাপ ----------
function onApp(ws) {
  const code = newCode();
  if (!code) {
    send(ws, { t: 'error', reason: 'busy' });
    return ws.close();
  }
  const s = { app: ws, tech: null, codeTimer: null, maxTimer: null };
  sessions.set(code, s);
  s.codeTimer = setTimeout(() => {
    if (!s.tech) endSession(code, 'expired');
  }, CODE_TTL_MS);
  s.maxTimer = setTimeout(() => endSession(code, 'timeout'), SESSION_MAX_MS);
  send(ws, { t: 'code', code, ttl: CODE_TTL_MS / 1000 });

  ws.on('message', (data, isBinary) => {
    const t = s.tech;
    if (!t || t.readyState !== 1) return;
    if (isBinary) t.send(data, { binary: true });
    else t.send(data.toString());
  });
  ws.on('close', () => endSession(code, 'app_closed'));
  ws.on('error', () => {});
}

// ---------- টেকনিশিয়ান ----------
function onTech(ws) {
  let code = null;
  const timer = setTimeout(() => {
    if (!code) ws.close(1000, 'no_join');
  }, 30000);

  ws.on('message', (data, isBinary) => {
    if (isBinary) return;
    let msg;
    try {
      msg = JSON.parse(data.toString());
    } catch (_) {
      return;
    }
    if (!code) {
      if (msg.t !== 'join') return;
      if (locked(ws.ip)) {
        send(ws, { t: 'error', reason: 'locked' });
        return ws.close();
      }
      if (!TECH_PASSWORD) {
        send(ws, { t: 'error', reason: 'server_not_configured' });
        return ws.close();
      }
      const s = sessions.get(String(msg.code || ''));
      if (!passwordOk(msg.pass || '') || !s || s.tech) {
        addFail(ws.ip);
        send(ws, { t: 'error', reason: !passwordOk(msg.pass || '') ? 'bad_password' : 'bad_code' });
        return ws.close();
      }
      code = String(msg.code);
      s.tech = ws;
      send(ws, { t: 'joined' });
      send(s.app, { t: 'joined' });
      return;
    }
    const s = sessions.get(code);
    if (!s || !s.app) return;
    if (msg.t === 'stop') return endSession(code, 'tech_stopped');
    if (['touch', 'text', 'key', 'next'].includes(msg.t)) {
      s.app.send(JSON.stringify(msg));
    }
  });
  ws.on('close', () => {
    clearTimeout(timer);
    if (code) endSession(code, 'tech_closed');
  });
  ws.on('error', () => {});
}

// জীবিত আছে কিনা দেখা (Render-এ সংযোগও ধরে রাখে)
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) {
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    ws.ping();
  }
}, 25000);

server.listen(PORT, () => {
  console.log('Green Net relay on port ' + PORT);
  if (!TECH_PASSWORD) console.log('WARNING: TECH_PASSWORD is not set');
});

// ---------- টেকনিশিয়ান পেজ ----------
const PANEL_HTML = `<!doctype html>
<html lang="bn"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Green Net রিমোট সাহায্য</title>
<style>
 body{margin:0;font-family:system-ui,sans-serif;background:#0B1220;color:#e5e7eb}
 .wrap{max-width:520px;margin:0 auto;padding:16px}
 h1{font-size:20px;color:#34D399;margin:8px 0 14px}
 input,button{font-size:16px;border-radius:12px;border:1px solid #1F3B30;padding:12px;background:#111B2E;color:#fff;box-sizing:border-box}
 input{width:100%;margin-bottom:10px}
 button{background:#3FA66B;border:0;font-weight:700;cursor:pointer}
 button.sec{background:#1B2A3E}
 button.red{background:#B91C1C}
 #msg{margin:10px 0;color:#FACC15;min-height:22px}
 #bar{display:flex;flex-wrap:wrap;gap:8px;margin:10px 0}
 #bar button{padding:10px 14px}
 #screenWrap{display:none}
 canvas{width:100%;max-width:420px;background:#000;border-radius:12px;touch-action:none;outline:none;display:block;margin:0 auto}
 #typeRow{display:flex;gap:8px;margin-top:10px}
 #typeRow input{margin:0}
</style></head><body><div class="wrap">
<h1>Green Net রিমোট সাহায্য</h1>
<div id="login">
 <input id="pass" type="password" placeholder="টেকনিশিয়ানের পাসওয়ার্ড" autocomplete="current-password">
 <input id="code" inputmode="numeric" maxlength="6" placeholder="গ্রাহকের ৬ সংখ্যার কোড">
 <button id="go" style="width:100%">যুক্ত হন</button>
</div>
<div id="msg"></div>
<div id="screenWrap">
 <canvas id="cv" tabindex="0" width="360" height="640"></canvas>
 <div id="bar">
  <button class="sec" id="back">⬅ Back</button>
  <button class="sec" id="enter">Enter</button>
  <button class="sec" id="del">⌫</button>
  <button class="red" id="stop">বন্ধ করুন</button>
 </div>
 <div id="typeRow"><input id="txt" placeholder="এখানে লিখে পাঠান (পাসওয়ার্ড ইত্যাদি)"><button id="sendTxt">পাঠান</button></div>
</div>
</div>
<script>
const $=id=>document.getElementById(id);
let ws=null, joined=false;
function say(s){$('msg').textContent=s}
const errs={bad_password:'পাসওয়ার্ড ভুল',bad_code:'কোড ভুল বা মেয়াদ শেষ',locked:'অনেকবার ভুল হয়েছে, কিছুক্ষণ পরে চেষ্টা করুন',server_not_configured:'সার্ভারে পাসওয়ার্ড সেট করা নেই'};
const ends={expired:'কোডের মেয়াদ শেষ',timeout:'সময় শেষ (৩০ মিনিট)',app_closed:'গ্রাহক বন্ধ করেছেন',tech_stopped:'আপনি বন্ধ করেছেন',tech_closed:'সংযোগ বন্ধ'};
$('go').onclick=()=>{
  const pass=$('pass').value, code=$('code').value.trim();
  if(!pass||code.length!==6){say('পাসওয়ার্ড ও ৬ সংখ্যার কোড দিন');return}
  say('যুক্ত হচ্ছে…');
  const proto=location.protocol==='https:'?'wss':'ws';
  ws=new WebSocket(proto+'://'+location.host+'/tech'); ws.binaryType='blob';
  ws.onopen=()=>ws.send(JSON.stringify({t:'join',pass,code}));
  ws.onmessage=async ev=>{
    if(typeof ev.data!=='string'){
      try{const bmp=await createImageBitmap(ev.data);const cv=$('cv');
        if(cv.width!==bmp.width||cv.height!==bmp.height){cv.width=bmp.width;cv.height=bmp.height}
        cv.getContext('2d').drawImage(bmp,0,0);bmp.close&&bmp.close();}catch(e){}
      ws.send(JSON.stringify({t:'next'}));return}
    const m=JSON.parse(ev.data);
    if(m.t==='joined'){joined=true;$('login').style.display='none';$('screenWrap').style.display='block';say('যুক্ত হয়েছে। গ্রাহকের স্ক্রিন আসছে…');ws.send(JSON.stringify({t:'next'}))}
    else if(m.t==='error'){say(errs[m.reason]||'সমস্যা: '+m.reason)}
    else if(m.t==='ended'){say(ends[m.reason]||'শেষ হয়েছে');joined=false}
  };
  ws.onclose=()=>{if(joined){say('সংযোগ কেটে গেছে');joined=false}};
};
function send(o){if(ws&&ws.readyState===1)ws.send(JSON.stringify(o))}
const cv=$('cv');
function pos(e){const r=cv.getBoundingClientRect();return{x:Math.min(1,Math.max(0,(e.clientX-r.left)/r.width)),y:Math.min(1,Math.max(0,(e.clientY-r.top)/r.height))}}
let down=false,lastMove=0;
cv.addEventListener('pointerdown',e=>{cv.focus();down=true;cv.setPointerCapture(e.pointerId);send({t:'touch',a:'down',...pos(e)});e.preventDefault()});
cv.addEventListener('pointermove',e=>{if(!down)return;const n=Date.now();if(n-lastMove<40)return;lastMove=n;send({t:'touch',a:'move',...pos(e)})});
const up=e=>{if(!down)return;down=false;send({t:'touch',a:'up',...pos(e)})};
cv.addEventListener('pointerup',up);cv.addEventListener('pointercancel',up);
cv.addEventListener('keydown',e=>{
  if(e.key==='Backspace'){send({t:'key',k:'del'});e.preventDefault()}
  else if(e.key==='Enter'){send({t:'key',k:'enter'});e.preventDefault()}
  else if(e.key.length===1&&!e.ctrlKey&&!e.metaKey){send({t:'text',s:e.key});e.preventDefault()}
});
$('back').onclick=()=>send({t:'key',k:'back'});
$('enter').onclick=()=>send({t:'key',k:'enter'});
$('del').onclick=()=>send({t:'key',k:'del'});
$('stop').onclick=()=>{send({t:'stop'});say('বন্ধ করা হয়েছে')};
$('sendTxt').onclick=()=>{const v=$('txt').value;if(v){send({t:'text',s:v});$('txt').value=''}};
</script></body></html>`;
