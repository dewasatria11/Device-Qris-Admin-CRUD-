var __defProp = Object.defineProperty;
var __name = (target, value) => __defProp(target, "name", { value, configurable: true });

// src/index.js
var corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, x-api-key, authorization, x-admin-key, x-device-token"
};
var noCacheHeaders = {
  ...corsHeaders,
  "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
  "Pragma": "no-cache",
  "Expires": "0"
};
var text = /* @__PURE__ */ __name((msg, status = 200) => new Response(msg, { status, headers: { ...noCacheHeaders, "x-worker-version": "v3-NOCACHE" } }), "text");
var json = /* @__PURE__ */ __name((data, status = 200) => new Response(JSON.stringify(data), {
  status,
  headers: { ...noCacheHeaders, "Content-Type": "application/json", "x-worker-version": "v3-NOCACHE" }
}), "json");
var getApiKey = /* @__PURE__ */ __name((req) => (req.headers.get("x-api-key") || req.headers.get("authorization") || "").trim() || null, "getApiKey");
var getAdminKey = /* @__PURE__ */ __name((req) => (req.headers.get("x-admin-key") || "").trim() || null, "getAdminKey");
var getDeviceToken = /* @__PURE__ */ __name((req) => (req.headers.get("x-device-token") || "").trim() || null, "getDeviceToken");
var heartbeatCols = null;
var heartbeatChecked = false;
var playedAtMigrationDone = false;
var alertMigrationDone = false;
var storeColsMigrationDone = false;
var WA_GATEWAYS = [
  { id: "wa", name: "WA 1 (wa)", url: "https://wa.servernyadewa.web.id/api/send" },
  { id: "wa2", name: "WA 2 (wa2)", url: "https://wa2.servernyadewa.web.id/api/send" },
  { id: "wa3", name: "WA 3 (wa3)", url: "https://wa3.servernyadewa.web.id/api/send" },
  { id: "wa4", name: "WA 4 (wa4)", url: "https://wa4.servernyadewa.web.id/api/send" },
  { id: "wa5", name: "WA 5 (wa5)", url: "https://wa5.servernyadewa.web.id/api/send" }
];
var WA_GATEWAY_SEND_URL = "https://wa.servernyadewa.web.id/api/send";
var WA_ALERT_NUMBERS = ["628561943396", "6281218768593"];
var WA_ALERT_TEXT = "SOUNDBOX NYA MATI (CEK / CAS)";

function normalizeGatewayUrl(gateway) {
  if (!gateway) return WA_GATEWAY_SEND_URL;
  const g = String(gateway).trim();
  const found = WA_GATEWAYS.find((item) => item.id === g.toLowerCase() || item.url === g);
  if (found) return found.url;
  if (g.startsWith("http://") || g.startsWith("https://")) {
    return g.endsWith("/api/send") ? g : (g.replace(/\/+$/, "") + "/api/send");
  }
  return WA_GATEWAY_SEND_URL;
}

function cleanPhone(raw) {
  let cleaned = String(raw || "").trim().replace(/[^0-9]/g, "");
  if (!cleaned) return "";
  if (cleaned.startsWith("08")) cleaned = "628" + cleaned.slice(2);
  else if (cleaned.startsWith("8")) cleaned = "628" + cleaned.slice(1);
  return cleaned;
}

function parsePhoneNumbers(input) {
  if (!input) return [];
  if (Array.isArray(input)) return input.map((s) => cleanPhone(s)).filter(Boolean);
  return String(input)
    .split(/[\r\n,;]+/)
    .map((s) => cleanPhone(s))
    .filter(Boolean);
}

async function ensureStoreColumns(env) {
  if (storeColsMigrationDone) return;
  const addColumn = async (sql, label) => {
    try {
      await env.DB.prepare(sql).run();
    } catch (e) {
      const msg = e?.message || String(e);
      if (!msg.includes("duplicate column") && !msg.includes("already exists")) {
        console.warn(`[DB] ensureStoreColumns ${label} notice:`, msg);
        throw e;
      }
    }
  };
  await addColumn(`ALTER TABLE stores ADD COLUMN wa_gateway TEXT`, "wa_gateway");
  await addColumn(`ALTER TABLE stores ADD COLUMN wa_target_numbers TEXT`, "wa_target_numbers");
  storeColsMigrationDone = true;
}
__name(ensureStoreColumns, "ensureStoreColumns");
async function ensurePlayedAt(env) {
  if (playedAtMigrationDone) return;
  playedAtMigrationDone = true;
  try {
    await env.DB.prepare(
      `ALTER TABLE transactions ADD COLUMN played_at TEXT`
    ).run();
  } catch (e) {
    // Column already exists - ignore
  }
}
__name(ensurePlayedAt, "ensurePlayedAt");
async function ensureAlertColumns(env) {
  if (alertMigrationDone) return;
  try {
    await env.DB.prepare(
      `ALTER TABLE device_heartbeat ADD COLUMN last_alert_at DATETIME`
    ).run();
    alertMigrationDone = true;
  } catch (e) {
    const msg = e?.message || String(e);
    if (msg.includes("duplicate column") || msg.includes("already exists")) {
      alertMigrationDone = true;
    } else {
      console.warn("[DB] ensureAlertColumns notice:", msg);
    }
  }
}
__name(ensureAlertColumns, "ensureAlertColumns");
async function getHeartbeatCols(env) {
  if (heartbeatChecked)
    return heartbeatCols;
  heartbeatChecked = true;
  try {
    const info = await env.DB.prepare(
      "PRAGMA table_info('device_heartbeat')"
    ).all();
    heartbeatCols = new Set((info.results || []).map((row) => row.name));
  } catch (e) {
    heartbeatCols = /* @__PURE__ */ new Set();
  }
  return heartbeatCols;
}
__name(getHeartbeatCols, "getHeartbeatCols");
function getHeartbeatJoinExpr(cols) {
  if (!cols || cols.size === 0)
    return null;
  if (cols.has("device_token"))
    return "s.device_token = d.device_token";
  if (cols.has("device_id"))
    return "s.device_token = d.device_id";
  if (cols.has("token"))
    return "s.device_token = d.token";
  if (cols.has("store_id"))
    return "s.store_id = d.store_id";
  return null;
}
__name(getHeartbeatJoinExpr, "getHeartbeatJoinExpr");
function pickHeartbeatKey(cols) {
  if (!cols || cols.size === 0)
    return null;
  if (cols.has("device_id"))
    return "device_id";
  if (cols.has("device_token"))
    return "device_token";
  if (cols.has("token"))
    return "token";
  if (cols.has("store_id"))
    return "store_id";
  return null;
}
__name(pickHeartbeatKey, "pickHeartbeatKey");
function makeTransactionId() {
  return "tx_" + Date.now() + "_" + randHex(4);
}
__name(makeTransactionId, "makeTransactionId");
function requireAdmin(request, env) {
  const adminKey = getAdminKey(request);
  const envAdmin = (env.ADMIN_KEY || "").trim();
  if (!adminKey || !envAdmin || adminKey !== envAdmin)
    return text("Unauthorized", 401);
  return null;
}
__name(requireAdmin, "requireAdmin");
function requireApi(request, env) {
  const apiKey = getApiKey(request);
  const envKey = (env.API_KEY || "").trim();
  if (!apiKey || !envKey || apiKey !== envKey)
    return text("Unauthorized", 401);
  return null;
}
__name(requireApi, "requireApi");
function randHex(bytes = 16) {
  const arr = new Uint8Array(bytes);
  crypto.getRandomValues(arr);
  return [...arr].map((b) => b.toString(16).padStart(2, "0")).join("");
}
__name(randHex, "randHex");
function normalizePath(pathname) {
  const p = (pathname || "").replace(/\/+$/, "");
  return p === "" ? "/" : p;
}
__name(normalizePath, "normalizePath");
async function sendWhatsApp(to, text2, gatewayUrl = WA_GATEWAY_SEND_URL) {
  const sendUrl = normalizeGatewayUrl(gatewayUrl);
  const res = await fetch(sendUrl, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ to, text: text2 })
  });
  const responseText = await res.text();
  if (!res.ok) {
    throw new Error(`WA send failed via ${sendUrl} to ${to}: HTTP ${res.status} ${responseText}`);
  }
  return responseText;
}
__name(sendWhatsApp, "sendWhatsApp");
async function sendAlert(device) {
  const storeLabel = device.name || device.store_id || "";
  const text2 = storeLabel ? `${WA_ALERT_TEXT}\nToko: ${storeLabel}` : WA_ALERT_TEXT;
  const gatewayUrl = normalizeGatewayUrl(device.wa_gateway);
  const targetNumbers = parsePhoneNumbers(device.wa_target_numbers);
  const recipients = targetNumbers.length ? targetNumbers : WA_ALERT_NUMBERS;
  console.log(`[ALERT] Store '${storeLabel}' offline. Gateway=${gatewayUrl}, recipients=${recipients.join(",")}`);
  const results = [];
  for (const to of recipients) {
    try {
      const responseText = await sendWhatsApp(to, text2, gatewayUrl);
      results.push({ to, gateway: gatewayUrl, ok: true, response: responseText });
      console.log(`[WA ALERT] sent via ${gatewayUrl} to ${to}`);
    } catch (e) {
      results.push({ to, gateway: gatewayUrl, ok: false, error: e?.message || String(e) });
      console.error(`[WA ALERT] failed via ${gatewayUrl} to ${to}:`, e);
    }
  }
  return results;
}
__name(sendAlert, "sendAlert");
var src_default = {
  async scheduled(event, env, ctx) {
    try {
      const cols = await getHeartbeatCols(env);
      const joinExpr = getHeartbeatJoinExpr(cols);
      if (!joinExpr) {
        console.log("Skip scheduled check: device_heartbeat key column not found.");
        return;
      }
      await ensureAlertColumns(env);
      await ensureStoreColumns(env);
      const thresholdMinutes = 1;
      const offlineDevices = await env.DB.prepare(`
        SELECT s.name, s.store_id, s.wa_gateway, s.wa_target_numbers, d.device_id, d.last_seen, d.last_alert_at
        FROM stores s
        JOIN device_heartbeat d ON ${joinExpr}
        WHERE s.enabled = 1 
          AND d.last_seen < datetime('now', '-' || ? || ' minutes')
          AND d.last_seen > datetime('now', '-24 hours')
          AND (d.last_alert_at IS NULL OR d.last_alert_at < d.last_seen)
      `).bind(thresholdMinutes).all();
      const devices = offlineDevices.results || [];
      for (const dev of devices) {
        const lastSeen = new Date(dev.last_seen).getTime();
        const now = Date.now();
        const diffMins = Math.max(1, Math.floor((now - lastSeen) / 6e4));
        ctx.waitUntil((async () => {
          try {
            await sendAlert({ ...dev, minutesOffline: diffMins });
            await env.DB.prepare(
              `UPDATE device_heartbeat SET last_alert_at=datetime('now') WHERE device_id=?`
            ).bind(dev.device_id).run();
            console.log(`[ALERT] last_alert_at updated for device ${dev.device_id}`);
          } catch (e) {
            console.error(`[ALERT] Failed to process alert for device ${dev.device_id || dev.store_id}:`, e);
          }
        })());
      }
      console.log(`Scheduled check done. Found ${devices.length} offline devices to alert.`);
    } catch (e) {
      console.error("Scheduled task error:", e);
    }
  },
  async fetch(request, env) {
    try {
      await ensurePlayedAt(env);
      await ensureAlertColumns(env);
      await ensureStoreColumns(env);
      if (request.method === "OPTIONS") {
        return new Response(null, { status: 204, headers: corsHeaders });
      }
      const url = new URL(request.url);
      const path = normalizePath(url.pathname);
      if (path === "/" && request.method === "GET") {
        return json({
          ok: true,
          service: "qris-router-multistore-1soundbox",
          endpoints: {
            admin_upsert_store: "POST /admin/stores",
            admin_list_stores: "GET /admin/stores",
            admin_enable: "POST /admin/stores/enable",
            admin_disable: "POST /admin/stores/disable",
            admin_delete: "POST /admin/stores/delete",
            admin_regen_token: "POST /admin/stores/regen-token",
            admin_list_transactions: "GET /admin/transactions?store_id=&played=&limit=",
            admin_clear_transactions: "POST /admin/transactions/clear {store_id}",
            admin_list_devices: "GET /admin/devices",
            admin_pair_device: "POST /admin/stores/pair {store_id, device_id}",
            admin_test_sound: "POST /admin/stores/test {store_id}",
            admin_set_wa_config: "POST /admin/stores/wa-config {store_id, wa_gateway, wa_target_numbers}",
            admin_test_wa_alert: "POST /admin/test-wa-alert {store_id?}",
            pair_check: "GET /pair-check?device_id=SOUNDBOX-XXXX",
            cashier_qris: "POST /qris {store_id, amount}",
            soundbox_poll: "GET /next-transaction?store_id=... (x-device-token)",
            website_check_payment: "GET /qris/check?store_id=&amount=&since="
          }
        });
      }
      if (path === "/admin/stores" && request.method === "POST") {
        const unauth = requireAdmin(request, env);
        if (unauth)
          return unauth;
        let body;
        try {
          body = await request.json();
        } catch {
          return text("Invalid JSON", 400);
        }
        const store_id = String(body.store_id || "").trim();
        const name = String(body.name || "").trim();
        const providedToken = String(body.device_token || "").trim();
        const wa_gateway = String(body.wa_gateway || "").trim();
        const wa_target_numbers = String(body.wa_target_numbers || "").trim();
        if (!store_id || !name)
          return text("store_id, name required", 400);
        if (!/^[a-z0-9._-]+$/i.test(store_id))
          return text("store_id invalid", 400);
        const existing = await env.DB.prepare(
          `SELECT device_token, wa_gateway, wa_target_numbers FROM stores WHERE store_id=? LIMIT 1`
        ).bind(store_id).first();
        const device_token = providedToken || existing?.device_token || `sb_${randHex(16)}`;
        const finalWaGateway = body.wa_gateway !== undefined ? wa_gateway : (existing?.wa_gateway || "");
        const finalWaTargetNumbers = body.wa_target_numbers !== undefined ? wa_target_numbers : (existing?.wa_target_numbers || "");
        await env.DB.prepare(
          `INSERT INTO stores (store_id, name, device_token, enabled, wa_gateway, wa_target_numbers)
           VALUES (?, ?, ?, 1, ?, ?)
           ON CONFLICT(store_id)
           DO UPDATE SET
             name=excluded.name,
             enabled=1,
             device_token=excluded.device_token,
             wa_gateway=excluded.wa_gateway,
             wa_target_numbers=excluded.wa_target_numbers`
        ).bind(store_id, name, device_token, finalWaGateway, finalWaTargetNumbers).run();
        return json({ ok: true, store_id, name, device_token, enabled: 1, wa_gateway: finalWaGateway, wa_target_numbers: finalWaTargetNumbers });
      }
      if (path === "/admin/stores/wa-config" && request.method === "POST") {
        const unauth = requireAdmin(request, env);
        if (unauth)
          return unauth;
        let body;
        try {
          body = await request.json();
        } catch {
          return text("Invalid JSON", 400);
        }
        const store_id = String(body.store_id || "").trim();
        const wa_gateway = String(body.wa_gateway || "").trim();
        const wa_target_numbers = String(body.wa_target_numbers || "").trim();
        if (!store_id)
          return text("store_id required", 400);
        const upd = await env.DB.prepare(
          `UPDATE stores SET wa_gateway=?, wa_target_numbers=? WHERE store_id=?`
        ).bind(wa_gateway, wa_target_numbers, store_id).run();
        const changes = upd?.meta?.changes ?? 0;
        if (changes !== 1)
          return text("Store not found", 404);
        return json({ ok: true, store_id, wa_gateway, wa_target_numbers });
      }
      if (path === "/admin/stores" && request.method === "GET") {
        const unauth = requireAdmin(request, env);
        if (unauth)
          return unauth;
        const rows = await env.DB.prepare(
          `SELECT store_id, name, enabled, created_at, device_token, wa_gateway, wa_target_numbers
           FROM stores
           ORDER BY created_at DESC`
        ).all();
        return json({ ok: true, stores: rows.results || [] });
      }
      if (path === "/admin/stores/enable" && request.method === "POST") {
        const unauth = requireAdmin(request, env);
        if (unauth)
          return unauth;
        let body;
        try {
          body = await request.json();
        } catch {
          return text("Invalid JSON", 400);
        }
        const store_id = String(body.store_id || "").trim();
        if (!store_id)
          return text("store_id required", 400);
        await env.DB.prepare(
          `UPDATE stores SET enabled=1 WHERE store_id=?`
        ).bind(store_id).run();
        return json({ ok: true, store_id, enabled: 1 });
      }
      if (path === "/admin/stores/disable" && request.method === "POST") {
        const unauth = requireAdmin(request, env);
        if (unauth)
          return unauth;
        let body;
        try {
          body = await request.json();
        } catch {
          return text("Invalid JSON", 400);
        }
        const store_id = String(body.store_id || "").trim();
        if (!store_id)
          return text("store_id required", 400);
        await env.DB.prepare(
          `UPDATE stores SET enabled=0 WHERE store_id=?`
        ).bind(store_id).run();
        return json({ ok: true, store_id, enabled: 0 });
      }
      if (path === "/admin/stores/delete" && request.method === "POST") {
        const unauth = requireAdmin(request, env);
        if (unauth)
          return unauth;
        let body;
        try {
          body = await request.json();
        } catch {
          return text("Invalid JSON", 400);
        }
        const store_id = String(body.store_id || "").trim();
        if (!store_id)
          return text("store_id required", 400);
        await env.DB.prepare(
          `DELETE FROM stores WHERE store_id=?`
        ).bind(store_id).run();
        return json({ ok: true, store_id, deleted: true });
      }
      if (path === "/admin/stores/regen-token" && request.method === "POST") {
        const unauth = requireAdmin(request, env);
        if (unauth)
          return unauth;
        let body;
        try {
          body = await request.json();
        } catch {
          return text("Invalid JSON", 400);
        }
        const store_id = String(body.store_id || "").trim();
        if (!store_id)
          return text("store_id required", 400);
        const newToken = `sb_${randHex(16)}`;
        const upd = await env.DB.prepare(
          `UPDATE stores SET device_token=? WHERE store_id=?`
        ).bind(newToken, store_id).run();
        const changes = upd?.meta?.changes ?? 0;
        if (changes !== 1)
          return text("Store not found", 404);
        return json({ ok: true, store_id, device_token: newToken });
      }
      if (path === "/admin/transactions" && request.method === "GET") {
        const unauth = requireAdmin(request, env);
        if (unauth)
          return unauth;
        const storeId = (url.searchParams.get("store_id") || "").trim();
        const playedParam = url.searchParams.get("played");
        const limitParam = url.searchParams.get("limit") || "50";
        const limit = Math.min(Math.max(parseInt(limitParam, 10) || 50, 1), 200);
        const where = [];
        const binds = [];
        if (storeId) {
          where.push("store_id = ?");
          binds.push(storeId);
        }
        if (playedParam === "0" || playedParam === "1") {
          where.push("played = ?");
          binds.push(Number(playedParam));
        }
        let sql = `
        SELECT id, transaction_id, store_id, amount, played, created_at, played_at
          FROM transactions
        `;
        if (where.length)
          sql += " WHERE " + where.join(" AND ");
        sql += " ORDER BY id DESC LIMIT ?";
        binds.push(limit);
        const rows = await env.DB.prepare(sql).bind(...binds).all();
        return json({ ok: true, transactions: rows.results || [] });
      }
      if (path === "/admin/transactions/clear" && request.method === "POST") {
        const unauth = requireAdmin(request, env);
        if (unauth)
          return unauth;
        let body;
        try {
          body = await request.json();
        } catch {
          return text("Invalid JSON", 400);
        }
        const storeId = String(body.store_id || "").trim();
        if (!storeId)
          return text("store_id required", 400);
        const res = await env.DB.prepare(
          `DELETE FROM transactions WHERE store_id = ? AND played = 0`
        ).bind(storeId).run();
        const deleted = res?.meta?.changes ?? 0;
        return json({ ok: true, store_id: storeId, deleted });
      }
      if (path === "/admin/devices" && request.method === "GET") {
        const unauth = requireAdmin(request, env);
        if (unauth)
          return unauth;
        const cols = await getHeartbeatCols(env);
        const joinExpr = getHeartbeatJoinExpr(cols);
        if (!joinExpr) {
          const result2 = await env.DB.prepare(`
            SELECT store_id, name, device_token, wa_gateway, wa_target_numbers,
                   NULL as last_seen, NULL as ip_address, NULL as firmware_version
            FROM stores
            ORDER BY created_at DESC
          `).all();
          return json({ ok: true, devices: result2.results || [] });
        }
        const result = await env.DB.prepare(`
          SELECT s.store_id, s.name, s.device_token, s.wa_gateway, s.wa_target_numbers,
                 d.last_seen, d.ip_address, d.firmware_version,
                 ROUND((julianday('now') - julianday(d.last_seen)) * 86400) AS age_seconds,
                 datetime('now') AS server_time
          FROM stores s
          LEFT JOIN device_heartbeat d ON ${joinExpr}
          ORDER BY d.last_seen DESC
        `).all();
        return json({ ok: true, devices: result.results || [] });
      }
      if (path === "/qris" && request.method === "POST") {
        const unauth = requireApi(request, env);
        if (unauth)
          return unauth;
        let body;
        try {
          body = await request.json();
        } catch {
          return text("Invalid JSON", 400);
        }
        const store_id = String(body.store_id || "").trim();
        const amount = Number(body.amount);
        if (!store_id)
          return text("store_id required", 400);
        if (!Number.isFinite(amount) || amount <= 0)
          return text("amount must be positive number", 400);
        const store = await env.DB.prepare(
          `SELECT enabled FROM stores WHERE store_id=? LIMIT 1`
        ).bind(store_id).first();
        if (!store || store.enabled !== 1)
          return text("Store disabled or not registered", 403);
        const transaction_id = makeTransactionId();
        await env.DB.prepare(
          `INSERT INTO transactions
           (transaction_id, store_id, amount, played)
           VALUES (?, ?, ?, 0)`
        ).bind(
          transaction_id,
          store_id,
          Math.floor(amount)
        ).run();
        return json({
          ok: true,
          transaction_id
        });
      }
      if (path === "/next-transaction" && request.method === "GET") {
        const store_id = String(url.searchParams.get("store_id") || "").trim();
        if (!store_id)
          return text("store_id required", 400);
        const token = getDeviceToken(request);
        if (!token)
          return text("x-device-token required", 401);
        const store = await env.DB.prepare(
          `SELECT enabled, device_token, name
           FROM stores
           WHERE store_id=? LIMIT 1`
        ).bind(store_id).first();
        if (!store || store.enabled !== 1)
          return text("Invalid or disabled store", 403);
        if (String(store.device_token || "").trim() !== token)
          return text("Unauthorized", 401);
        try {
          const ip = request.headers.get("CF-Connecting-IP") || "0.0.0.0";
          const version = request.headers.get("User-Agent") || "Unknown";
          const cols = await getHeartbeatCols(env);
          const keyCol = pickHeartbeatKey(cols);
          if (keyCol) {
            const insertCols = [];
            const placeholders = [];
            const values = [];
            const addCol = /* @__PURE__ */ __name((name, value, placeholder = "?") => {
              insertCols.push(name);
              placeholders.push(placeholder);
              if (placeholder === "?")
                values.push(value);
            }, "addCol");
            const tokenKey = keyCol === "store_id" ? store_id : token;
            addCol(keyCol, tokenKey);
            if (cols.has("store_id") && keyCol !== "store_id") {
              addCol("store_id", store_id);
            }
            if (cols.has("last_seen"))
              addCol("last_seen", null, "CURRENT_TIMESTAMP");
            if (cols.has("ip_address"))
              addCol("ip_address", ip);
            if (cols.has("firmware_version"))
              addCol("firmware_version", version);
            const updates = [];
            if (cols.has("last_seen"))
              updates.push("last_seen=excluded.last_seen");
            if (cols.has("ip_address"))
              updates.push("ip_address=excluded.ip_address");
            if (cols.has("firmware_version"))
              updates.push("firmware_version=excluded.firmware_version");
            if (cols.has("store_id") && keyCol !== "store_id")
              updates.push("store_id=excluded.store_id");
            let sql = `INSERT INTO device_heartbeat (${insertCols.join(", ")}) VALUES (${placeholders.join(", ")})`;
            if (updates.length) {
              sql += ` ON CONFLICT(${keyCol}) DO UPDATE SET ${updates.join(", ")}`;
            }
            await env.DB.prepare(sql).bind(...values).run();
          }
        } catch (err) {
          console.error("Heartbeat error:", err);
        }
        for (let i = 0; i < 3; i++) {
          const row = await env.DB.prepare(
            `SELECT id, transaction_id, amount
             FROM transactions
             WHERE played=0 
               AND store_id=?
               AND created_at > datetime('now', '-5 minutes') -- Only play fresh transactions
             ORDER BY id ASC
             LIMIT 1`
          ).bind(store_id).first();
          if (!row)
            break;
          const upd = await env.DB.prepare(
            `UPDATE transactions
             SET played=1,
                 played_at=datetime('now')
             WHERE id=? AND played=0`
          ).bind(row.id).run();
          const changes = upd?.meta?.changes ?? 0;
          if (changes === 1) {
            return json({
              transaction_id: row.transaction_id,
              amount: row.amount,
              store_id,
              store_name: store.name
            });
          }
        }
        return json({ available: false, store_name: store.name || "UNKNOWN-NAME" });
      }
      if (path === "/qris/check" && request.method === "GET") {
        const store_id = String(url.searchParams.get("store_id") || "").trim();
        const amount = Number(url.searchParams.get("amount"));
        const since = String(url.searchParams.get("since") || "").trim();
        if (!store_id)
          return text("store_id required", 400);
        if (!Number.isFinite(amount) || amount <= 0)
          return text("amount must be positive number", 400);
        if (!since)
          return text("since required", 400);
        const sinceDate = new Date(since);
        if (isNaN(sinceDate.getTime()))
          return text("since must be a valid ISO date", 400);

        const row = await env.DB.prepare(
          `SELECT id, transaction_id, amount, created_at
           FROM transactions
           WHERE store_id=?
             AND amount=?
             AND claimed=0
             AND created_at >= ?
           ORDER BY id ASC
           LIMIT 1`
        ).bind(store_id, Math.floor(amount), sinceDate.toISOString().replace("T", " ").replace("Z", "")).first();

        if (!row) {
          return json({ ok: true, paid: false });
        }

        const upd = await env.DB.prepare(
          `UPDATE transactions
           SET claimed=1
           WHERE id=? AND claimed=0`
        ).bind(row.id).run();
        const changes = upd?.meta?.changes ?? 0;

        if (changes !== 1) {
          // Race condition: another request already claimed it a moment ago.
          return json({ ok: true, paid: false });
        }

        return json({
          ok: true,
          paid: true,
          transaction_id: row.transaction_id,
          amount: row.amount,
          paid_at: row.created_at
        });
      }
      if (path === "/admin/stores/pair" && request.method === "POST") {
        const unauth = requireAdmin(request, env);
        if (unauth)
          return unauth;
        let body;
        try {
          body = await request.json();
        } catch {
          return text("Invalid JSON", 400);
        }
        const store_id = String(body.store_id || "").trim();
        const device_id = String(body.device_id || "").trim();
        if (!store_id || !device_id)
          return text("store_id and device_id required", 400);
        const store = await env.DB.prepare(
          `SELECT device_token, name FROM stores WHERE store_id=? LIMIT 1`
        ).bind(store_id).first();
        if (!store)
          return text("Store not found", 404);
        const device_token = store.device_token || "";
        const store_name = store.name || "";
        try {
          await env.DB.prepare(`
            CREATE TABLE IF NOT EXISTS pending_pairs (
              device_id TEXT PRIMARY KEY,
              store_id TEXT NOT NULL,
              device_token TEXT NOT NULL,
              store_name TEXT DEFAULT '',
              created_at TEXT DEFAULT (datetime('now'))
            )
          `).run();
        } catch (e) {
        }
        try {
          await env.DB.prepare(
            `DELETE FROM pending_pairs WHERE created_at < datetime('now', '-30 days')`
          ).run();
        } catch (e) {
        }
        await env.DB.prepare(
          `INSERT INTO pending_pairs (device_id, store_id, device_token, store_name)
           VALUES (?, ?, ?, ?)
           ON CONFLICT(device_id)
           DO UPDATE SET store_id=excluded.store_id,
                        device_token=excluded.device_token,
                        store_name=excluded.store_name,
                        created_at=datetime('now')`
        ).bind(device_id, store_id, device_token, store_name).run();
        return json({
          ok: true,
          device_id,
          store_id,
          store_name,
          message: `Pending pair created. Device ${device_id} will auto-pair on next poll.`
        });
      }
      if (path === "/admin/stores/test" && request.method === "POST") {
        const unauth = requireAdmin(request, env);
        if (unauth)
          return unauth;
        let body;
        try {
          body = await request.json();
        } catch {
          return text("Invalid JSON", 400);
        }
        const store_id = String(body.store_id || "").trim();
        if (!store_id)
          return text("store_id required", 400);
        const store = await env.DB.prepare(
          `SELECT enabled, name FROM stores WHERE store_id=? LIMIT 1`
        ).bind(store_id).first();
        if (!store)
          return text("Store not found", 404);
        const transaction_id = makeTransactionId();
        await env.DB.prepare(
          `INSERT INTO transactions
           (transaction_id, store_id, amount, played)
           VALUES (?, ?, 1, 0)`
        ).bind(transaction_id, store_id).run();
        return json({
          ok: true,
          message: `Test sound sent to ${store.name || store_id}`,
          transaction_id
        });
      }
      if (path === "/admin/test-wa-alert" && request.method === "POST") {
        const unauth = requireAdmin(request, env);
        if (unauth)
          return unauth;
        let body = {};
        try {
          body = await request.json();
        } catch {}
        let store = null;
        const reqStoreId = String(body?.store_id || "").trim();
        if (reqStoreId) {
          store = await env.DB.prepare(
            `SELECT store_id, name, wa_gateway, wa_target_numbers FROM stores WHERE store_id=? LIMIT 1`
          ).bind(reqStoreId).first();
        }
        const wa_gateway = body?.wa_gateway || store?.wa_gateway || "";
        const wa_target_numbers = body?.wa_target_numbers || store?.wa_target_numbers || "";
        const results = await sendAlert({
          name: store?.name || body?.name || (reqStoreId ? reqStoreId : "TEST ALERT"),
          store_id: store?.store_id || reqStoreId || "test-wa-alert",
          wa_gateway,
          wa_target_numbers,
          minutesOffline: 1,
          last_seen: new Date().toISOString()
        });
        const parsedTargets = parsePhoneNumbers(wa_target_numbers);
        const recipients = parsedTargets.length ? parsedTargets : WA_ALERT_NUMBERS;
        return json({
          ok: results.every((r) => r.ok),
          message: WA_ALERT_TEXT,
          gateway: normalizeGatewayUrl(wa_gateway),
          recipients,
          results
        });
      }
      if (path === "/pair-check" && request.method === "GET") {
        const device_id = String(url.searchParams.get("device_id") || "").trim();
        if (!device_id)
          return json({ paired: false, error: "device_id required" });
        let row = null;
        try {
          row = await env.DB.prepare(
            `SELECT store_id, device_token, store_name
             FROM pending_pairs
             WHERE device_id=? LIMIT 1`
          ).bind(device_id).first();
        } catch (e) {
          return json({ paired: false });
        }
        if (!row)
          return json({ paired: false });
        try {
          await env.DB.prepare(
            `DELETE FROM pending_pairs WHERE device_id=?`
          ).bind(device_id).run();
        } catch (e) {
        }
        return json({
          paired: true,
          store_id: row.store_id,
          device_token: row.device_token,
          store_name: row.store_name || ""
        });
      }
      return text("Not found", 404);
    } catch (e) {
      console.log("FATAL:", e?.stack || e?.message || e);
      return json({ ok: false, error: e?.message, stack: e?.stack }, 500);
    }
  }
};
export {
  src_default as default
};
//# sourceMappingURL=index.js.map