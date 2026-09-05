async function getJSON(url) {
  const r = await fetch(url, { headers: { Accept: 'application/json' } });
  const d = await r.json();
  if (!r.ok) throw new Error(d.error || r.statusText);
  return d;
}

async function postJSON(url, body) {
  const r = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
    body: JSON.stringify(body),
  });
  const d = await r.json();
  if (!r.ok) throw new Error(d.error || r.statusText);
  return d;
}

function debounce(fn, ms) {
  let t;
  return (...a) => {
    clearTimeout(t);
    t = setTimeout(() => fn(...a), ms);
  };
}

function fmtNum(v) {
  if (v == null || v === '') return '—';
  const n = Number(v);
  if (!isFinite(n)) return String(v);
  return n.toLocaleString('en-US', { maximumFractionDigits: 2 });
}

function fmtUSD(v) {
  if (v == null || v === '') return '—';
  const n = Number(v);
  if (!isFinite(n)) return String(v);
  const abs = Math.abs(n);
  const s = n < 0 ? '-' : '';
  if (abs >= 1e12) return `${s}$${(abs / 1e12).toFixed(2)}T`;
  if (abs >= 1e9) return `${s}$${(abs / 1e9).toFixed(2)}B`;
  if (abs >= 1e6) return `${s}$${(abs / 1e6).toFixed(1)}M`;
  if (abs >= 1e3) return `${s}$${(abs / 1e3).toFixed(0)}K`;
  return `${s}$${abs.toFixed(2)}`;
}

function escapeHtml(s) {
  return String(s ?? '').replace(/[&<>"']/g, c => (
    { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]
  ));
}

// inline markdown: **bold**, `code`, newlines -> <br>
function mdInline(s) {
  return escapeHtml(s)
    .replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>')
    .replace(/`([^`]+)`/g, '<code>$1</code>')
    .replace(/\n/g, '<br>');
}

// block markdown: ##/### headings, - / * bullets, paragraphs
function renderMarkdown(src) {
  const lines = String(src ?? '').split('\n');
  let html = '', inList = false;
  const closeList = () => { if (inList) { html += '</ul>'; inList = false; } };
  for (let raw of lines) {
    const line = raw.trimEnd();
    let m;
    if ((m = line.match(/^#{2,4}\s+(.*)$/))) {
      closeList(); html += `<h3>${escapeHtml(m[1])}</h3>`;
    } else if ((m = line.match(/^\s*[-*]\s+(.*)$/))) {
      if (!inList) { html += '<ul>'; inList = true; }
      html += `<li>${mdInline(m[1])}</li>`;
    } else if (line === '') {
      closeList(); html += '';
    } else {
      closeList(); html += `<p>${mdInline(line)}</p>`;
    }
  }
  closeList();
  return html;
}
