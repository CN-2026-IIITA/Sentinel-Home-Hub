/* ══════════════════════════════════════════════════════════════
   Smart Home Hub — Live Architecture Visualization
   Canvas-based animated network topology with particle system
   ══════════════════════════════════════════════════════════════ */
// ── DOM Elements ────────────────────────────────────────────────
const topologyCanvas = document.getElementById('topologyCanvas');
const topologyCtx = topologyCanvas.getContext('2d');
const chartCanvas = document.getElementById('throughputChart');
const chartCtx = chartCanvas.getContext('2d');

const statusDot = document.getElementById('statusDot');
const statusText = document.getElementById('statusText');
const msgCountEl = document.getElementById('msgCount');
const uptimeEl = document.getElementById('uptime');
const throughputValEl = document.getElementById('throughputVal');
const queueFeed = document.getElementById('queueFeed');
const messageFeed = document.getElementById('messageFeed');
const feedCountEl = document.getElementById('feedCount');
const hexDisplay = document.getElementById('hexDisplay');
const decodedFields = document.getElementById('decodedFields');
const offsetSlider = document.getElementById('offsetSlider');
const offsetCurrent = document.getElementById('offsetCurrent');
const offsetMax = document.getElementById('offsetMax');

// ── State ───────────────────────────────────────────────────────
let totalMessages = 0;
let startTime = Date.now();
let maxLogOffset = 0;
const throughputHistory = Array(60).fill(0);
let currentThroughput = 0;
let maxThroughput = 50;

// ── Node Definitions ────────────────────────────────────────────
const COLORS = {
    fire: '#f43f5e', temperature: '#06b6d4', door: '#f59e0b',
    light: '#a855f7', battery: '#10b981', broker: '#6366f1',
    phone: '#10b981', dashboard: '#0ea5e9', alert: '#f97316',
};

const NODES = {
    temperature: { x: 0.07, y: 0.15, icon: '🌡️', label: 'Temperature', color: COLORS.temperature, type: 'pub' },
    fire: { x: 0.07, y: 0.38, icon: '🔥', label: 'Fire Alarm', color: COLORS.fire, type: 'pub' },
    door: { x: 0.07, y: 0.61, icon: '🚪', label: 'Door Sensor', color: COLORS.door, type: 'pub' },
    light: { x: 0.07, y: 0.84, icon: '💡', label: 'Smart Light', color: COLORS.light, type: 'pub' },
    broker: { x: 0.46, y: 0.48, icon: '🏠', label: 'Smart Home Hub', color: COLORS.broker, type: 'broker', sublabel: 'Port 9999 (TCP)' },
    phone: { x: 0.88, y: 0.18, icon: '📱', label: 'Phone App', color: COLORS.phone, type: 'sub' },
    dashboard: { x: 0.88, y: 0.50, icon: '🖥️', label: 'Dashboard', color: COLORS.dashboard, type: 'sub' },
    alertSys: { x: 0.88, y: 0.82, icon: '⚠️', label: 'Alert System', color: COLORS.alert, type: 'sub' },
};

// Connections: publisher → broker, broker → subscribers
const CONNECTIONS = [
    ['temperature', 'broker'], ['fire', 'broker'], ['door', 'broker'], ['light', 'broker'],
    ['broker', 'phone'], ['broker', 'dashboard'], ['broker', 'alertSys'],
];

// Topic → publisher node mapping
const TOPIC_NODE_MAP = {
    'home/fire': 'fire', 'home/temperature': 'temperature',
    'home/door': 'door', 'home/light': 'light', 'home/battery': 'temperature',
};

// ── Particle System ─────────────────────────────────────────────
const particles = [];
const nodeGlows = {};  // nodeKey → { intensity, color, decayRate }

class Particle {
    constructor(fromKey, toKey, color, priority) {
        this.fromKey = fromKey;
        this.toKey = toKey;
        this.color = color;
        this.priority = priority;
        this.progress = 0;
        this.speed = 0.014 + (priority / 255) * 0.012;
        this.size = 3 + (priority / 255) * 5;
        this.trail = [];
        this.alive = true;
        this.onArrive = null;
    }

    update(w, h) {
        this.progress += this.speed;
        if (this.progress >= 1) {
            this.alive = false;
            if (this.onArrive) this.onArrive();
            return;
        }
        const pos = this.getPos(w, h);
        this.trail.push({ x: pos.x, y: pos.y });
        if (this.trail.length > 12) this.trail.shift();
    }

    getPos(w, h) {
        const f = NODES[this.fromKey];
        const t = NODES[this.toKey];
        const fx = f.x * w, fy = f.y * h;
        const tx = t.x * w, ty = t.y * h;
        const mx = (fx + tx) / 2;
        const my = (fy + ty) / 2 + (fy - ty) * 0.15;
        const p = this.progress;
        const ip = 1 - p;
        return {
            x: ip * ip * fx + 2 * ip * p * mx + p * p * tx,
            y: ip * ip * fy + 2 * ip * p * my + p * p * ty,
        };
    }

    draw(ctx, w, h) {
        // Trail
        for (let i = 0; i < this.trail.length; i++) {
            const alpha = (i + 1) / this.trail.length * 0.5;
            const sz = this.size * (i + 1) / this.trail.length * 0.6;
            ctx.beginPath();
            ctx.arc(this.trail[i].x, this.trail[i].y, sz, 0, Math.PI * 2);
            ctx.fillStyle = this.color + Math.round(alpha * 255).toString(16).padStart(2, '0');
            ctx.fill();
        }
        // Main dot
        const pos = this.getPos(w, h);
        ctx.beginPath();
        ctx.arc(pos.x, pos.y, this.size, 0, Math.PI * 2);
        ctx.fillStyle = this.color;
        ctx.fill();
        // Glow
        ctx.beginPath();
        ctx.arc(pos.x, pos.y, this.size * 3, 0, Math.PI * 2);
        const grd = ctx.createRadialGradient(pos.x, pos.y, 0, pos.x, pos.y, this.size * 3);
        grd.addColorStop(0, this.color + '60');
        grd.addColorStop(1, this.color + '00');
        ctx.fillStyle = grd;
        ctx.fill();
    }
}

function spawnMessage(topic, priority, payload) {
    const pubKey = TOPIC_NODE_MAP[topic] || 'temperature';
    const color = NODES[pubKey]?.color || '#6366f1';

    // Glow the publisher node
    nodeGlows[pubKey] = { intensity: 1, color: color, decay: 0.02 };

    // Step 1: publisher → broker
    const p1 = new Particle(pubKey, 'broker', color, priority);
    p1.onArrive = () => {
        // Glow the broker
        nodeGlows['broker'] = { intensity: 1, color: color, decay: 0.025 };
        // Step 2: broker → all subscribers (fan-out)
        const subs = ['phone', 'dashboard', 'alertSys'];
        subs.forEach((sub, i) => {
            setTimeout(() => {
                const p2 = new Particle('broker', sub, color, priority);
                p2.onArrive = () => {
                    nodeGlows[sub] = { intensity: 1, color: color, decay: 0.03 };
                };
                particles.push(p2);
            }, i * 40);
        });
    };
    particles.push(p1);
}

// ── Canvas Drawing ──────────────────────────────────────────────
function resizeTopology() {
    const container = document.getElementById('topologyContainer');
    const rect = container.getBoundingClientRect();
    topologyCanvas.width = rect.width * devicePixelRatio;
    topologyCanvas.height = rect.height * devicePixelRatio;
    topologyCanvas.style.width = rect.width + 'px';
    topologyCanvas.style.height = rect.height + 'px';
    topologyCtx.setTransform(devicePixelRatio, 0, 0, devicePixelRatio, 0, 0);
}

function drawTopology() {
    const w = topologyCanvas.width / devicePixelRatio;
    const h = topologyCanvas.height / devicePixelRatio;
    const ctx = topologyCtx;
    ctx.clearRect(0, 0, w, h);

    // Background grid
    ctx.strokeStyle = 'rgba(255,255,255,0.015)';
    ctx.lineWidth = 1;
    for (let x = 0; x < w; x += 40) { ctx.beginPath(); ctx.moveTo(x, 0); ctx.lineTo(x, h); ctx.stroke(); }
    for (let y = 0; y < h; y += 40) { ctx.beginPath(); ctx.moveTo(0, y); ctx.lineTo(w, y); ctx.stroke(); }

    // Draw connections
    CONNECTIONS.forEach(([fromKey, toKey]) => {
        const f = NODES[fromKey], t = NODES[toKey];
        const fx = f.x * w, fy = f.y * h;
        const tx = t.x * w, ty = t.y * h;
        const mx = (fx + tx) / 2;
        const my = (fy + ty) / 2 + (fy - ty) * 0.15;

        ctx.beginPath();
        ctx.moveTo(fx, fy);
        ctx.quadraticCurveTo(mx, my, tx, ty);
        ctx.strokeStyle = 'rgba(255,255,255,0.06)';
        ctx.lineWidth = 1.5;
        ctx.stroke();

        // "TCP" label at midpoint
        const lx = fx * 0.45 + tx * 0.55;
        const ly = fy * 0.45 + ty * 0.55 + (fy - ty) * 0.06;
        ctx.font = '9px "JetBrains Mono", monospace';
        ctx.fillStyle = 'rgba(255,255,255,0.12)';
        ctx.textAlign = 'center';
        ctx.fillText('TCP', lx, ly);
    });

    // Draw section labels
    ctx.font = '600 10px Inter, sans-serif';
    ctx.textAlign = 'center';
    ctx.fillStyle = 'rgba(255,255,255,0.15)';
    ctx.fillText('PUBLISHERS', 0.07 * w, h - 10);
    ctx.fillText('BROKER', 0.46 * w, h - 10);
    ctx.fillText('SUBSCRIBERS', 0.88 * w, h - 10);

    // Draw particles
    for (let i = particles.length - 1; i >= 0; i--) {
        particles[i].update(w, h);
        if (!particles[i].alive) { particles.splice(i, 1); continue; }
        particles[i].draw(ctx, w, h);
    }

    // Draw nodes
    Object.entries(NODES).forEach(([key, node]) => {
        const nx = node.x * w, ny = node.y * h;
        const glow = nodeGlows[key];
        const isBroker = node.type === 'broker';
        const radius = isBroker ? 38 : 24;

        // Glow ring
        if (glow && glow.intensity > 0.05) {
            ctx.beginPath();
            ctx.arc(nx, ny, radius + 12, 0, Math.PI * 2);
            const g = ctx.createRadialGradient(nx, ny, radius, nx, ny, radius + 20);
            g.addColorStop(0, glow.color + Math.round(glow.intensity * 100).toString(16).padStart(2, '0'));
            g.addColorStop(1, glow.color + '00');
            ctx.fillStyle = g;
            ctx.fill();
            glow.intensity -= glow.decay;
        }

        // Node circle
        ctx.beginPath();
        ctx.arc(nx, ny, radius, 0, Math.PI * 2);
        ctx.fillStyle = isBroker ? 'rgba(99,102,241,0.15)' : 'rgba(255,255,255,0.04)';
        ctx.fill();
        ctx.strokeStyle = node.color + '50';
        ctx.lineWidth = isBroker ? 2 : 1.5;
        ctx.stroke();

        // Icon
        ctx.font = isBroker ? '22px sans-serif' : '18px sans-serif';
        ctx.textAlign = 'center';
        ctx.textBaseline = 'middle';
        ctx.fillText(node.icon, nx, ny - (isBroker ? 4 : 0));

        // Label
        ctx.font = '600 11px Inter, sans-serif';
        ctx.fillStyle = '#e2e8f0';
        ctx.textBaseline = 'top';
        ctx.fillText(node.label, nx, ny + radius + 6);

        if (node.sublabel) {
            ctx.font = '500 9px "JetBrains Mono", monospace';
            ctx.fillStyle = 'rgba(255,255,255,0.3)';
            ctx.fillText(node.sublabel, nx, ny + radius + 22);
        }

        // Broker internal stages
        if (isBroker) {
            ctx.font = '500 8px "JetBrains Mono", monospace';
            ctx.fillStyle = 'rgba(255,255,255,0.25)';
            ctx.fillText('Decode → QoS → Log → Fanout', nx, ny + 12);
        }
    });
}

// ── Throughput Chart ────────────────────────────────────────────
function resizeChart() {
    const wrap = chartCanvas.parentElement;
    chartCanvas.width = wrap.clientWidth * devicePixelRatio;
    chartCanvas.height = wrap.clientHeight * devicePixelRatio;
    chartCanvas.style.width = wrap.clientWidth + 'px';
    chartCanvas.style.height = wrap.clientHeight + 'px';
    chartCtx.setTransform(devicePixelRatio, 0, 0, devicePixelRatio, 0, 0);
}

function drawChart() {
    const w = chartCanvas.width / devicePixelRatio;
    const h = chartCanvas.height / devicePixelRatio;
    const ctx = chartCtx;
    ctx.clearRect(0, 0, w, h);

    const currentMax = Math.max(...throughputHistory, 20);
    maxThroughput = maxThroughput * 0.92 + currentMax * 0.08;

    ctx.beginPath();
    const step = w / (throughputHistory.length - 1);
    for (let i = 0; i < throughputHistory.length; i++) {
        const x = i * step;
        const y = h - (throughputHistory[i] / maxThroughput) * h * 0.9;
        if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    }
    ctx.strokeStyle = '#6366f1';
    ctx.lineWidth = 2;
    ctx.stroke();

    ctx.lineTo(w, h);
    ctx.lineTo(0, h);
    const grad = ctx.createLinearGradient(0, 0, 0, h);
    grad.addColorStop(0, 'rgba(129,140,248,0.25)');
    grad.addColorStop(1, 'rgba(129,140,248,0.0)');
    ctx.fillStyle = grad;
    ctx.fill();
}

// ── Priority Queue Display ──────────────────────────────────────
const queueItems = [];
const colors = { critical: '#f43f5e', high: '#f59e0b', medium: '#6366f1', low: '#10b981' };
const bgs = { critical: 'rgba(244,63,94,0.1)', high: 'rgba(245,158,11,0.1)', medium: 'rgba(99,102,241,0.08)', low: 'rgba(16,185,129,0.08)' };
let queueUpdateTimer = null;

function addToQueue(topic, priority, payload) {
    const priLabel = priority >= 200 ? 'critical' : priority >= 128 ? 'high' : priority >= 64 ? 'medium' : 'low';

    queueItems.unshift({ topic, priority, priLabel, payload });
    if (queueItems.length > 20) queueItems.pop();

    if (queueUpdateTimer) return;
    queueUpdateTimer = setTimeout(() => {
        queueUpdateTimer = null;
        // Sort by priority (highest first) for display
        const sorted = [...queueItems].sort((a, b) => b.priority - a.priority);

        queueFeed.innerHTML = sorted.map(item => `
            <div class="queue-item" style="background:${bgs[item.priLabel]}">
                <div class="q-dot" style="background:${colors[item.priLabel]};box-shadow:0 0 6px ${colors[item.priLabel]}60"></div>
                <span class="q-pri" style="color:${colors[item.priLabel]}">${item.priority}</span>
                <span class="q-topic">${item.topic}</span>
            </div>
        `).join('');
    }, 50);
}

// ── Message Feed ────────────────────────────────────────────────
function addFeedMessage(topic, priority, payload, timestamp, isHistory) {
    const priClass = priority >= 200 ? 'pri-critical' : priority >= 128 ? 'pri-high' : priority >= 64 ? 'pri-medium' : 'pri-low';
    const time = timestamp ? new Date(timestamp * 1000).toLocaleTimeString('en-US', { hour12: false }) : '--:--:--';
    const topicShort = topic.replace('home/', '').replace('broker/', '');
    const histClass = isHistory ? ' history' : '';

    const emptyEl = messageFeed.querySelector('.feed-empty');
    if (emptyEl) emptyEl.remove();

    const el = document.createElement('div');
    el.className = `feed-msg ${priClass}${histClass}`;
    el.innerHTML = `
        <span class="feed-pri">${priority}</span>
        <span class="feed-topic">${topicShort}</span>
        <span class="feed-payload">${escapeHtml(payload)}</span>
        <span class="feed-time">${time}</span>
    `;
    messageFeed.insertBefore(el, messageFeed.firstChild);
    while (messageFeed.children.length > 100) messageFeed.removeChild(messageFeed.lastChild);

    totalMessages++;
    msgCountEl.textContent = totalMessages;
    feedCountEl.textContent = totalMessages + ' messages';
}

function escapeHtml(text) {
    const d = document.createElement('div');
    d.textContent = text;
    return d.innerHTML;
}

// ── Protocol Inspector ──────────────────────────────────────────
function updateInspector(hex, topic, priority, payload) {
    // Format hex with spaces
    const formatted = hex.match(/.{1,2}/g)?.join(' ') || hex;
    hexDisplay.textContent = formatted.toUpperCase();

    const cmdByte = hex.substring(0, 2).toUpperCase();
    const topicHex = hex.substring(2, 10).toUpperCase();
    const priByte = hex.substring(10, 12).toUpperCase();
    const lenHex = hex.substring(12, 20).toUpperCase();
    const payloadHex = hex.substring(20).toUpperCase();

    const cmdNames = { '01': 'SUBSCRIBE', '02': 'PUBLISH', '03': 'TIME_TRAVEL' };

    decodedFields.innerHTML = `
        <div class="decoded-row"><span class="d-label">CMD</span><span class="d-value">0x${cmdByte} (${cmdNames[cmdByte] || '?'})</span></div>
        <div class="decoded-row"><span class="d-label">TOPIC</span><span class="d-value">0x${topicHex} → ${topic}</span></div>
        <div class="decoded-row"><span class="d-label">PRI</span><span class="d-value">0x${priByte} (${priority})</span></div>
        <div class="decoded-row"><span class="d-label">LENGTH</span><span class="d-value">0x${lenHex} (${payload.length} bytes)</span></div>
        <div class="decoded-row"><span class="d-label">PAYLOAD</span><span class="d-value" style="color:var(--temp)">"${escapeHtml(payload.substring(0, 40))}"</span></div>
        <div class="decoded-row"><span class="d-label">TOTAL</span><span class="d-value">${hex.length / 2} bytes (JSON: ~${payload.length + 50} bytes → ${Math.round((1 - hex.length / 2 / (payload.length + 50)) * 100)}% smaller)</span></div>
    `;
}

// ── SSE Connection ──────────────────────────────────────────────
function connectSSE() {
    const src = new EventSource('/stream');

    src.onopen = () => {
        statusDot.className = 'status-dot connected';
        statusText.textContent = 'Dashboard Server Connected';
    };

    src.onmessage = (e) => {
        try {
            const data = JSON.parse(e.data);

            if (data.type === 'connection_status') {
                if (data.connected) {
                    statusDot.className = 'status-dot connected';
                    statusText.textContent = 'Broker Connected';
                } else {
                    statusDot.className = 'status-dot disconnected';
                    statusText.textContent = 'Broker Disconnected';
                }
            }
            else if (data.type === 'metrics') {
                currentThroughput = data.throughput || 0;
                throughputValEl.textContent = currentThroughput;
                throughputHistory.push(currentThroughput);
                throughputHistory.shift();
                drawChart();

                if (data.current_log_offset > maxLogOffset) {
                    maxLogOffset = data.current_log_offset;
                    offsetSlider.max = maxLogOffset;
                    offsetMax.textContent = formatBytes(maxLogOffset);
                }
            }
            else if (data.type === 'qos' || data.type === 'time_travel_log') {
                const topic = data.topic || 'unknown';
                const pri = data.priority || 0;
                const payload = data.payload || '';
                const ts = data.timestamp || Date.now() / 1000;
                const isHistory = data.history === true;

                // Animate particles only for live messages to prevent browser freeze
                if (!isHistory) {
                    spawnMessage(topic, pri, payload);
                    addToQueue(topic, pri, payload);
                }

                // Update UI log feed
                addFeedMessage(topic, pri, payload, ts, isHistory);

                // Update protocol inspector
                if (data.hex && !isHistory) {
                    updateInspector(data.hex, topic, pri, payload);
                }
            }
            else if (data.type === 'time_travel_done') {
                const btn = document.getElementById('btnReplay');
                btn.textContent = `✅ Replayed ${data.replayed || 0} msgs`;
                btn.style.opacity = '1';
                setTimeout(() => { btn.textContent = '⏪ Replay History'; }, 2000);
            }
        } catch (err) {
            console.error('SSE parse error:', err);
        }
    };

    src.onerror = () => {
        statusDot.className = 'status-dot disconnected';
        statusText.textContent = 'Disconnected — Retrying…';
        src.close();
        setTimeout(connectSSE, 2000);
    };
}

// ── Device Control Buttons ──────────────────────────────────────
document.querySelectorAll('.device-btn').forEach(btn => {
    btn.addEventListener('click', async () => {
        const device = btn.dataset.device;
        btn.style.transform = 'scale(0.93)';
        setTimeout(() => btn.style.transform = '', 150);

        try {
            await fetch('/api/simulate', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ device }),
            });
        } catch (err) {
            console.error('Simulate error:', err);
        }
    });
});

// ── Time Travel ─────────────────────────────────────────────────
offsetSlider.addEventListener('input', (e) => {
    offsetCurrent.textContent = formatBytes(parseInt(e.target.value));
});

document.getElementById('btnReplay').addEventListener('click', async () => {
    const btn = document.getElementById('btnReplay');
    const offset = parseInt(offsetSlider.value);
    btn.textContent = '⏳ Replaying…';
    btn.style.opacity = '0.6';
    btn.disabled = true;
    try {
        await fetch('/api/time-travel', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ offset }),
        });
    } catch (err) { console.error('Time travel error:', err); }
    // Button reset is handled by the 'time_travel_done' SSE event
    // Fallback timeout in case the event doesn't arrive
    setTimeout(() => {
        btn.disabled = false;
        if (btn.textContent === '⏳ Replaying…') {
            btn.textContent = '⏪ Replay History';
            btn.style.opacity = '1';
        }
    }, 15000);
});

function formatBytes(bytes) {
    if (bytes === 0) return '0B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + sizes[i];
}

// ── Uptime Counter ──────────────────────────────────────────────
setInterval(() => {
    const secs = Math.floor((Date.now() - startTime) / 1000);
    const m = Math.floor(secs / 60), s = secs % 60;
    uptimeEl.textContent = m > 0 ? `${m}m ${s}s` : `${s}s`;
}, 1000);

// ── Animation Loop ──────────────────────────────────────────────
function animate() {
    drawTopology();
    requestAnimationFrame(animate);
}

// ── Init ────────────────────────────────────────────────────────
function init() {
    resizeTopology();
    resizeChart();
    drawChart();
    connectSSE();
    animate();
}

window.addEventListener('resize', () => { resizeTopology(); resizeChart(); drawChart(); });
window.addEventListener('load', init);
