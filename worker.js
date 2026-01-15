const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type,x-admin-key"
};

function jsonResponse(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json", ...corsHeaders }
  });
}

function textResponse(message, status = 200) {
  return new Response(message, { status, headers: corsHeaders });
}

function unauthorized() {
  return textResponse("Unauthorized", 401);
}

function badRequest(message) {
  return jsonResponse({ ok: false, error: message }, 400);
}

function isAdmin(request, env) {
  const key = request.headers.get("x-admin-key") || "";
  return Boolean(env.ADMIN_KEY) && key === env.ADMIN_KEY;
}

function randomHex(len = 32) {
  const bytes = new Uint8Array(len / 2);
  crypto.getRandomValues(bytes);
  return [...bytes].map((b) => b.toString(16).padStart(2, "0")).join("");
}

async function readJson(request) {
  try {
    return await request.json();
  } catch {
    return {};
  }
}

async function handleAdmin(request, env, url) {
  const { pathname } = url;

  if (pathname === "/admin/stores" && request.method === "GET") {
    const result = await env.DB.prepare(
      "SELECT store_id, name, device_token, enabled, created_at FROM stores ORDER BY created_at DESC"
    ).all();
    return jsonResponse({ ok: true, stores: result.results || [] });
  }

  if (pathname === "/admin/stores" && request.method === "POST") {
    const body = await readJson(request);
    const store_id = typeof body.store_id === "string" ? body.store_id.trim() : "";
    const name = typeof body.name === "string" ? body.name.trim() : "";
    const incomingToken = typeof body.device_token === "string" ? body.device_token.trim() : "";
    if (!store_id || !name) return badRequest("store_id and name required");

    const existing = await env.DB.prepare(
      "SELECT device_token FROM stores WHERE store_id = ?"
    ).bind(store_id).first();
    const device_token = incomingToken || (existing && existing.device_token) || `sb_${randomHex(32)}`;

    if (existing) {
      await env.DB.prepare(
        "UPDATE stores SET name = ?, device_token = ? WHERE store_id = ?"
      ).bind(name, device_token, store_id).run();
    } else {
      await env.DB.prepare(
        "INSERT INTO stores (store_id, name, device_token, enabled, created_at) VALUES (?, ?, ?, 1, CURRENT_TIMESTAMP)"
      ).bind(store_id, name, device_token).run();
    }

    return jsonResponse({ ok: true, store_id, name, device_token });
  }

  if (pathname === "/admin/stores/enable" && request.method === "POST") {
    const body = await readJson(request);
    const store_id = typeof body.store_id === "string" ? body.store_id.trim() : "";
    if (!store_id) return badRequest("store_id required");
    await env.DB.prepare("UPDATE stores SET enabled = 1 WHERE store_id = ?").bind(store_id).run();
    return jsonResponse({ ok: true });
  }

  if (pathname === "/admin/stores/disable" && request.method === "POST") {
    const body = await readJson(request);
    const store_id = typeof body.store_id === "string" ? body.store_id.trim() : "";
    if (!store_id) return badRequest("store_id required");
    await env.DB.prepare("UPDATE stores SET enabled = 0 WHERE store_id = ?").bind(store_id).run();
    return jsonResponse({ ok: true });
  }

  if (pathname === "/admin/stores/delete" && request.method === "POST") {
    const body = await readJson(request);
    const store_id = typeof body.store_id === "string" ? body.store_id.trim() : "";
    if (!store_id) return badRequest("store_id required");
    await env.DB.prepare("DELETE FROM stores WHERE store_id = ?").bind(store_id).run();
    return jsonResponse({ ok: true });
  }

  if (pathname === "/admin/transactions" && request.method === "GET") {
    const store_id = url.searchParams.get("store_id") || "";
    const playedParam = url.searchParams.get("played");
    const limitParam = parseInt(url.searchParams.get("limit") || "50", 10);
    const limit = Number.isFinite(limitParam) ? Math.min(Math.max(limitParam, 1), 200) : 50;

    const clauses = [];
    const binds = [];

    if (store_id) {
      clauses.push("store_id = ?");
      binds.push(store_id);
    }
    if (playedParam === "0" || playedParam === "1") {
      clauses.push("played = ?");
      binds.push(Number(playedParam));
    }

    let sql = "SELECT id, store_id, amount, played, created_at FROM transactions";
    if (clauses.length) sql += " WHERE " + clauses.join(" AND ");
    sql += " ORDER BY id DESC LIMIT ?";
    binds.push(limit);

    const result = await env.DB.prepare(sql).bind(...binds).all();
    return jsonResponse({ ok: true, transactions: result.results || [] });
  }

  if (pathname === "/admin/transactions/clear" && request.method === "POST") {
    const body = await readJson(request);
    const store_id = typeof body.store_id === "string" ? body.store_id.trim() : "";
    if (!store_id) return badRequest("store_id required");

    const result = await env.DB.prepare(
      "DELETE FROM transactions WHERE store_id = ?"
    ).bind(store_id).run();
    return jsonResponse({ ok: true, deleted: result.meta && result.meta.changes ? result.meta.changes : 0 });
  }

  return textResponse("Not found", 404);
}

export default {
  async fetch(request, env) {
    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: corsHeaders });
    }

    const url = new URL(request.url);

    try {
      if (url.pathname.startsWith("/admin/")) {
        if (!isAdmin(request, env)) return unauthorized();
        return await handleAdmin(request, env, url);
      }

      return textResponse("Not found", 404);
    } catch (err) {
      return jsonResponse({ ok: false, error: err && err.message ? err.message : "Server error" }, 500);
    }
  }
};
