const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  "Access-Control-Allow-Headers":
    "Content-Type, x-api-key, authorization, x-admin-key, x-device-token",
};

const json = (data, status = 200) =>
  new Response(JSON.stringify(data), {
    status,
    headers: { ...corsHeaders, "Content-Type": "application/json" },
  });

const text = (msg, status = 200) =>
  new Response(msg, { status, headers: corsHeaders });

const getApiKey = (req) =>
  (req.headers.get("x-api-key") || req.headers.get("authorization") || "")
    .trim() || null;

const getAdminKey = (req) =>
  (req.headers.get("x-admin-key") || "").trim() || null;

const getDeviceToken = (req) =>
  (req.headers.get("x-device-token") || "").trim() || null;

let heartbeatCols = null;
let heartbeatChecked = false;

async function getHeartbeatCols(env) {
  if (heartbeatChecked) return heartbeatCols;
  heartbeatChecked = true;

  try {
    const info = await env.DB.prepare(
      "PRAGMA table_info('device_heartbeat')"
    ).all();
    heartbeatCols = new Set((info.results || []).map((row) => row.name));
  } catch (e) {
    heartbeatCols = new Set();
  }

  return heartbeatCols;
}

function getHeartbeatJoinExpr(cols) {
  if (!cols || cols.size === 0) return null;
  if (cols.has("device_token")) return "s.device_token = d.device_token";
  if (cols.has("device_id")) return "s.device_token = d.device_id";
  if (cols.has("token")) return "s.device_token = d.token";
  if (cols.has("store_id")) return "s.store_id = d.store_id";
  return null;
}

function pickHeartbeatKey(cols) {
  if (!cols || cols.size === 0) return null;
  if (cols.has("device_id")) return "device_id";
  if (cols.has("device_token")) return "device_token";
  if (cols.has("token")) return "token";
  if (cols.has("store_id")) return "store_id";
  return null;
}

function makeTransactionId() {
  return "tx_" + Date.now() + "_" + randHex(4);
}

function requireAdmin(request, env) {
  const adminKey = getAdminKey(request);
  const envAdmin = (env.ADMIN_KEY || "").trim();
  if (!adminKey || !envAdmin || adminKey !== envAdmin) return text("Unauthorized", 401);
  return null;
}

function requireApi(request, env) {
  const apiKey = getApiKey(request);
  const envKey = (env.API_KEY || "").trim();
  if (!apiKey || !envKey || apiKey !== envKey) return text("Unauthorized", 401);
  return null;
}

function randHex(bytes = 16) {
  const arr = new Uint8Array(bytes);
  crypto.getRandomValues(arr);
  return [...arr].map((b) => b.toString(16).padStart(2, "0")).join("");
}

function normalizePath(pathname) {
  const p = (pathname || "").replace(/\/+$/, "");
  return p === "" ? "/" : p;
}

async function sendAlert(storeName, minutesOffline) {
  // Placeholder for real alert logic (Telegram/WhatsApp/Email)
  console.log(`[ALERT] Store '${storeName}' offline for ${minutesOffline} mins.`);
  // Example: await fetch('https://api.telegram.org/botTOKEN/sendMessage', { ... })
}

export default {
  async scheduled(event, env, ctx) {
    // Check for devices offline > 30 mins
    try {
      const cols = await getHeartbeatCols(env);
      const joinExpr = getHeartbeatJoinExpr(cols);
      if (!joinExpr) {
        console.log("Skip scheduled check: device_heartbeat key column not found.");
        return;
      }
      const thresholdMinutes = 30;
      const offlineDevices = await env.DB.prepare(`
        SELECT s.name, s.store_id, d.last_seen
        FROM stores s
        JOIN device_heartbeat d ON ${joinExpr}
        WHERE s.enabled = 1 
          AND d.last_seen < datetime('now', '-' || ? || ' minutes')
          AND d.last_seen > datetime('now', '-24 hours') -- Avoid alerting for long dead devices continuously
      `).bind(thresholdMinutes).all();

      const devices = offlineDevices.results || [];
      for (const dev of devices) {
        // Calculate approx minutes offline
        const lastSeen = new Date(dev.last_seen).getTime();
        const now = Date.now();
        const diffMins = Math.floor((now - lastSeen) / 60000);

        ctx.waitUntil(sendAlert(dev.name, diffMins));
      }
      console.log(`Scheduled check done. Found ${devices.length} offline devices.`);
    } catch (e) {
      console.error("Scheduled task error:", e);
    }
  },

  async fetch(request, env) {
    try {
      if (request.method === "OPTIONS") {
        return new Response(null, { status: 204, headers: corsHeaders });
      }

      const url = new URL(request.url);
      const path = normalizePath(url.pathname);

      // ===============================
      // Health
      // ===============================
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
            admin_list_devices: "GET /admin/devices", // NEW
            cashier_qris: "POST /qris {store_id, amount}",
            soundbox_poll: "GET /next-transaction?store_id=... (x-device-token)",
          },
        });
      }

      // ===============================
      // ADMIN: UPSERT STORE
      // POST /admin/stores
      // Body: { store_id, name, device_token? }
      // - If device_token not provided: generate on create, keep existing on update
      // - Enabled set to 1 on upsert
      // ===============================
      if (path === "/admin/stores" && request.method === "POST") {
        const unauth = requireAdmin(request, env);
        if (unauth) return unauth;

        let body;
        try { body = await request.json(); }
        catch { return text("Invalid JSON", 400); }

        const store_id = String(body.store_id || "").trim();
        const name = String(body.name || "").trim();
        const providedToken = String(body.device_token || "").trim();

        if (!store_id || !name) return text("store_id, name required", 400);
        if (!/^[a-z0-9._-]+$/i.test(store_id)) return text("store_id invalid", 400);

        const existing = await env.DB.prepare(
          `SELECT device_token FROM stores WHERE store_id=? LIMIT 1`
        ).bind(store_id).first();

        const device_token =
          providedToken ||
          existing?.device_token ||
          `sb_${randHex(16)}`;

        await env.DB.prepare(
          `INSERT INTO stores (store_id, name, device_token, enabled)
           VALUES (?, ?, ?, 1)
           ON CONFLICT(store_id)
           DO UPDATE SET
             name=excluded.name,
             enabled=1,
             device_token=excluded.device_token`
        ).bind(store_id, name, device_token).run();

        return json({ ok: true, store_id, name, device_token, enabled: 1 });
      }

      // ===============================
      // ADMIN: LIST STORES
      // GET /admin/stores
      // ===============================
      if (path === "/admin/stores" && request.method === "GET") {
        const unauth = requireAdmin(request, env);
        if (unauth) return unauth;

        const rows = await env.DB.prepare(
          `SELECT store_id, name, enabled, created_at, device_token
           FROM stores
           ORDER BY created_at DESC`
        ).all();

        return json({ ok: true, stores: rows.results || [] });
      }

      // ===============================
      // ADMIN: ENABLE/DISABLE/DELETE
      // ===============================
      if (path === "/admin/stores/enable" && request.method === "POST") {
        const unauth = requireAdmin(request, env);
        if (unauth) return unauth;

        let body;
        try { body = await request.json(); }
        catch { return text("Invalid JSON", 400); }

        const store_id = String(body.store_id || "").trim();
        if (!store_id) return text("store_id required", 400);

        await env.DB.prepare(
          `UPDATE stores SET enabled=1 WHERE store_id=?`
        ).bind(store_id).run();

        return json({ ok: true, store_id, enabled: 1 });
      }

      if (path === "/admin/stores/disable" && request.method === "POST") {
        const unauth = requireAdmin(request, env);
        if (unauth) return unauth;

        let body;
        try { body = await request.json(); }
        catch { return text("Invalid JSON", 400); }

        const store_id = String(body.store_id || "").trim();
        if (!store_id) return text("store_id required", 400);

        await env.DB.prepare(
          `UPDATE stores SET enabled=0 WHERE store_id=?`
        ).bind(store_id).run();

        return json({ ok: true, store_id, enabled: 0 });
      }

      if (path === "/admin/stores/delete" && request.method === "POST") {
        const unauth = requireAdmin(request, env);
        if (unauth) return unauth;

        let body;
        try { body = await request.json(); }
        catch { return text("Invalid JSON", 400); }

        const store_id = String(body.store_id || "").trim();
        if (!store_id) return text("store_id required", 400);

        await env.DB.prepare(
          `DELETE FROM stores WHERE store_id=?`
        ).bind(store_id).run();

        return json({ ok: true, store_id, deleted: true });
      }

      // ===============================
      // ADMIN: REGENERATE TOKEN
      // POST /admin/stores/regen-token
      // Body: { store_id }
      // ===============================
      if (path === "/admin/stores/regen-token" && request.method === "POST") {
        const unauth = requireAdmin(request, env);
        if (unauth) return unauth;

        let body;
        try { body = await request.json(); }
        catch { return text("Invalid JSON", 400); }

        const store_id = String(body.store_id || "").trim();
        if (!store_id) return text("store_id required", 400);

        const newToken = `sb_${randHex(16)}`;

        const upd = await env.DB.prepare(
          `UPDATE stores SET device_token=? WHERE store_id=?`
        ).bind(newToken, store_id).run();

        const changes = upd?.meta?.changes ?? 0;
        if (changes !== 1) return text("Store not found", 404);

        return json({ ok: true, store_id, device_token: newToken });
      }

      // ===============================
      // ADMIN: LIST TRANSACTIONS
      // GET /admin/transactions?store_id=&played=&limit=
      // ===============================
      if (path === "/admin/transactions" && request.method === "GET") {
        const unauth = requireAdmin(request, env);
        if (unauth) return unauth;

        const storeId = (url.searchParams.get("store_id") || "").trim();
        const playedParam = url.searchParams.get("played"); // "0"|"1"|null
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
        SELECT id, transaction_id, store_id, amount, played, created_at
          FROM transactions
        `;
        if (where.length) sql += " WHERE " + where.join(" AND ");
        sql += " ORDER BY id DESC LIMIT ?";

        binds.push(limit);

        const rows = await env.DB.prepare(sql).bind(...binds).all();
        return json({ ok: true, transactions: rows.results || [] });
      }

      // ===============================
      // ADMIN: CLEAR TRANSACTIONS (per store)
      // POST /admin/transactions/clear
      // Body: { store_id }
      // ===============================
      if (path === "/admin/transactions/clear" && request.method === "POST") {
        const unauth = requireAdmin(request, env);
        if (unauth) return unauth;

        let body;
        try { body = await request.json(); }
        catch { return text("Invalid JSON", 400); }

        const storeId = String(body.store_id || "").trim();
        if (!storeId) return text("store_id required", 400);

        const res = await env.DB.prepare(
          `DELETE FROM transactions WHERE store_id = ?`
        ).bind(storeId).run();

        const deleted = res?.meta?.changes ?? 0;
        return json({ ok: true, store_id: storeId, deleted });
      }

      // ===============================
      // ADMIN: LIST DEVICES (NEW)
      // GET /admin/devices
      // ===============================
      if (path === "/admin/devices" && request.method === "GET") {
        const unauth = requireAdmin(request, env);
        if (unauth) return unauth;

        const cols = await getHeartbeatCols(env);
        const joinExpr = getHeartbeatJoinExpr(cols);
        if (!joinExpr) {
          const result = await env.DB.prepare(`
            SELECT store_id, name, device_token,
                   NULL as last_seen, NULL as ip_address, NULL as firmware_version
            FROM stores
            ORDER BY created_at DESC
          `).all();
          return json({ ok: true, devices: result.results || [] });
        }

        const result = await env.DB.prepare(`
          SELECT s.store_id, s.name, s.device_token, 
                 d.last_seen, d.ip_address, d.firmware_version
          FROM stores s
          LEFT JOIN device_heartbeat d ON ${joinExpr}
          ORDER BY d.last_seen DESC
        `).all();
        return json({ ok: true, devices: result.results || [] });
      }

      // ===============================
      // KASIR: POST /qris
      // Body: { store_id, amount }
      // ===============================
      if (path === "/qris" && request.method === "POST") {
        const unauth = requireApi(request, env);
        if (unauth) return unauth;

        let body;
        try { body = await request.json(); }
        catch { return text("Invalid JSON", 400); }

        const store_id = String(body.store_id || "").trim();
        const amount = Number(body.amount);

        if (!store_id) return text("store_id required", 400);
        if (!Number.isFinite(amount) || amount <= 0) return text("amount must be positive number", 400);

        const store = await env.DB.prepare(
          `SELECT enabled FROM stores WHERE store_id=? LIMIT 1`
        ).bind(store_id).first();

        if (!store || store.enabled !== 1) return text("Store disabled or not registered", 403);

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
          transaction_id,
        });
      }

      // ===============================
      // SOUNDBOX: GET /next-transaction?store_id=...
      // Auth: x-device-token
      // ===============================
      if (path === "/next-transaction" && request.method === "GET") {
        const store_id = String(url.searchParams.get("store_id") || "").trim();
        if (!store_id) return text("store_id required", 400);

        const token = getDeviceToken(request);
        if (!token) return text("x-device-token required", 401);

        const store = await env.DB.prepare(
          `SELECT enabled, device_token
           FROM stores
           WHERE store_id=? LIMIT 1`
        ).bind(store_id).first();

        if (!store || store.enabled !== 1) return text("Invalid or disabled store", 403);
        if (String(store.device_token || "").trim() !== token) return text("Unauthorized", 401);

        // RECORD HEARTBEAT
        // We do this every time the device polls /next-transaction
        try {
          const ip = request.headers.get("CF-Connecting-IP") || "0.0.0.0";
          const version = request.headers.get("User-Agent") || "Unknown"; // Assuming UA carries fw version, or add custom header

          // Upsert heartbeat
          const cols = await getHeartbeatCols(env);
          const keyCol = pickHeartbeatKey(cols);
          if (keyCol) {
            const insertCols = [];
            const placeholders = [];
            const values = [];

            const addCol = (name, value, placeholder = "?") => {
              insertCols.push(name);
              placeholders.push(placeholder);
              if (placeholder === "?") values.push(value);
            };

            const tokenKey = (keyCol === "store_id") ? store_id : token;
            addCol(keyCol, tokenKey);

            if (cols.has("store_id") && keyCol !== "store_id") {
              addCol("store_id", store_id);
            }
            if (cols.has("last_seen")) addCol("last_seen", null, "CURRENT_TIMESTAMP");
            if (cols.has("ip_address")) addCol("ip_address", ip);
            if (cols.has("firmware_version")) addCol("firmware_version", version);

            const updates = [];
            if (cols.has("last_seen")) updates.push("last_seen=excluded.last_seen");
            if (cols.has("ip_address")) updates.push("ip_address=excluded.ip_address");
            if (cols.has("firmware_version")) updates.push("firmware_version=excluded.firmware_version");
            if (cols.has("store_id") && keyCol !== "store_id") updates.push("store_id=excluded.store_id");

            let sql = `INSERT INTO device_heartbeat (${insertCols.join(", ")}) VALUES (${placeholders.join(", ")})`;
            if (updates.length) {
              sql += ` ON CONFLICT(${keyCol}) DO UPDATE SET ${updates.join(", ")}`;
            }

            await env.DB.prepare(sql).bind(...values).run();
          }
        } catch (err) {
          console.error("Heartbeat error:", err);
          // Don't fail the transaction poll just because heartbeat failed
        }

        for (let i = 0; i < 3; i++) {
          const row = await env.DB.prepare(
            `SELECT id, transaction_id, amount
             FROM transactions
             WHERE played=0 AND store_id=?
             ORDER BY id ASC
             LIMIT 1`
          ).bind(store_id).first();

          if (!row) return json({ available: false });

          const upd = await env.DB.prepare(
            `UPDATE transactions
             SET played=1
             WHERE id=? AND played=0`
          ).bind(row.id).run();

          const changes = upd?.meta?.changes ?? 0;
          if (changes === 1) {
            return json({
              available: true,
              transaction_id: row.transaction_id,
              amount: row.amount,
              store_id,
            });

          }
        }

        return json({ available: false });
      }

      return text("Not found", 404);
    } catch (e) {
      console.log("FATAL:", e?.stack || e?.message || e);
      return json({ ok: false, error: e?.message, stack: e?.stack }, 500);
    }
  },
};
