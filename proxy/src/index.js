// Proxy: meneruskan semua request dari URL lama (workers.dev) ke domain baru.
// Dipakai agar soundbox yang belum di-flash ulang tetap terhubung ke server baru.
const TARGET = "https://soundbox.servernyadewa.web.id";

export default {
  async fetch(request) {
    const url = new URL(request.url);
    const target = new URL(url.pathname + url.search, TARGET);
    const req = new Request(target.toString(), request);
    return fetch(req);
  },
};
