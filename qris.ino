#include <ArduinoJson.h>
#include <DNSServer.h>
#include <HTTPClient.h>
#include <Preferences.h>
#include <WebServer.h>
#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <time.h>

#include <LiquidCrystal_I2C.h>
#include <Wire.h>

#include <DFRobotDFPlayerMini.h>

// =====================
// USER CONFIG
// =====================
static const char *WORKER_BASE = "https://server.soundboxqris123.workers.dev";

// I2C pins (ESP32 default SDA=21 SCL=22)
#define I2C_SDA 21
#define I2C_SCL 22
static const uint8_t LCD_ADDR =
    0x27; // LCD I2C address (try 0x3F if not working)
static const uint32_t I2C_CLOCK_HZ = 100000; // 100kHz

// DFPlayer pins
static const int DF_RX = 2; // ESP32 RX  <- DFPlayer TX
static const int DF_TX = 4; // ESP32 TX  -> DFPlayer RX

// Time config
static const char *ntpServer = "pool.ntp.org";
static const long gmtOffset_sec = 25200; // WIB (UTC+7)
static const int daylightOffset_sec = 0;

// Reset config
#define EN_RESET_PIN 0      // Pin used for EN/boot button (GPIO 0)
#define RESET_WINDOW_MS 900 // Max time between presses
#define RESET_COUNT 3       // How many presses to trigger reset
#define MIN_PRESS_MS 50     // Debounce

// Captive DNS
static const byte DNS_PORT = 53;

// LCD payment screen timings
static const uint32_t LCD_STEP_MS = 2000;
static const uint32_t LCD_AMOUNT_MS = 2000;

// =====================
// EN triple reset detection (FLASH-based, survives hard reset)
// =====================
static const uint32_t EN_WINDOW_MS = 6000; // 6 seconds window
static const uint32_t EN_REQUIRED = 3;     // 3 resets => factory reset

// We'll store in flash (Preferences) because RTC doesn't survive POWERON_RESET
// Preferences keys:
// - "enCount" = reset count
// - "enTime" = millis() at last reset (boot time reference)
// - "enBoot" = boot number (increments each boot to detect actual reboots)

// =====================
// Globals
// =====================
LiquidCrystal_I2C lcd(LCD_ADDR, 16, 2); // LCD 16x2 I2C
bool lcdOk = false;

HardwareSerial DFSerial(2);
DFRobotDFPlayerMini df;
bool dfOk = false;

// HTTP Server Object
WebServer server(80);

// HTTP timeout for outgoing requests
static const uint32_t HTTP_TIMEOUT_MS = 10000;

DNSServer dnsServer;
Preferences prefs;

String wifiSsid, wifiPass;
String storeId, deviceToken, storeName;

bool apOn = false;
bool timeSynced = false;

uint32_t nextPollAt = 0;
uint32_t backoffMs = 5000;
const uint32_t POLL_MS = 5000;
const uint32_t MAX_BACKOFF = 60000;
uint8_t pollFailStreak = 0;

// LCD idle animation state
uint32_t lastIdleDraw = 0;
int16_t idleScrollX = 0;
bool idleBlink = false;
static const uint32_t IDLE_ANIM_MS = 500; // 500ms for LCD scrolling

enum PollResult { POLL_GOT_TX, POLL_NO_TX, POLL_ERROR };
PollResult pollOnce();

// Function Prototypes
void handleRoot();
void handleSaveWifi();
void handlePair();
void handleStatus();
void handleTestWorker();
void handleFactory();
void handleRestart();
void handleHeap();
void handleNotFound();

// =====================
// Helpers
// =====================
String cut16(String s) {
  s.trim();
  if (s.length() > 16)
    s = s.substring(0, 16);
  return s;
}

void lcd2(String l1, String l2) {
  if (!lcdOk)
    return;
  lcd.clear();
  lcd.setCursor(0, 0);
  lcd.print(cut16(l1));
  lcd.setCursor(0, 1);
  lcd.print(cut16(l2));
}

bool initLCD() {
  Wire.begin(I2C_SDA, I2C_SCL);
  Wire.setClock(I2C_CLOCK_HZ);

  lcd.init();
  lcd.backlight();

  // Test if LCD responds
  lcd.setCursor(0, 0);
  lcd.print("BOOT...");
  delay(100);

  Serial.println("[LCD] OK");
  lcd2("BOOT", "");
  return true;
}

// =====================
// Preferences
// =====================
void loadPrefs() {
  wifiSsid = prefs.getString("wifiSsid", "");
  wifiPass = prefs.getString("wifiPass", "");
  storeId = prefs.getString("storeId", "");
  deviceToken = prefs.getString("deviceToken", "");
  storeName = prefs.getString("storeName", ""); // Load store name
}

void saveWifi(const String &ssid, const String &pass) {
  prefs.putString("wifiSsid", ssid);
  prefs.putString("wifiPass", pass);
  wifiSsid = ssid;
  wifiPass = pass;
}

void savePair(const String &sid, const String &tok, const String &name = "") {
  prefs.putString("storeId", sid);
  prefs.putString("deviceToken", tok);
  if (name.length() > 0)
    prefs.putString("storeName", name);

  storeId = sid;
  deviceToken = tok;
  if (name.length() > 0)
    storeName = name;
}

bool isPaired() { return storeId.length() > 0 && deviceToken.length() > 0; }

void clearPairing() {
  prefs.putString("storeId", "");
  prefs.putString("deviceToken", "");
  prefs.putString("storeName", "");
  storeId = "";
  deviceToken = "";
  storeName = "";
}

void clearWifi() {
  prefs.putString("wifiSsid", "");
  prefs.putString("wifiPass", "");
  wifiSsid = "";
  wifiPass = "";
}

void factoryResetNow(const char *reason) {
  // Clear ALL preferences (WiFi, pairing, EN counter, everything)
  prefs.clear();

  // Reset in-memory variables too
  storeId = "";
  deviceToken = "";
  storeName = "";
  wifiSsid = "";
  wifiPass = "";

  Serial.print("[FACTORY] cleared WiFi + pairing. reason=");
  Serial.println(reason);

  if (lcdOk) {
    lcd2("FACTORY", "RESET");
    delay(1000);
  }

  // Play "Reset Berhasil" (0009.mp3)
  // Asumsi durasi 3-4 detik cukup
  if (dfOk) {
    playTrack(9, 4000);
  }

  ESP.restart();
}

// =====================
// EN triple reset logic (FLASH-based)
// Uses boot counter + timestamp to detect 3 quick resets
// =====================
void handleENTripleResetEarly() {
  // Get current values from flash
  uint32_t enCount = prefs.getUInt("enCount", 0);
  uint32_t enBoot = prefs.getUInt("enBoot", 0);
  uint32_t enTime = prefs.getUInt("enTime", 0);

  // Current boot number (increment on every boot)
  uint32_t currentBoot = enBoot + 1;
  prefs.putUInt("enBoot", currentBoot);

  // Estimate: if this is the first boot within EN_WINDOW_MS since last tracked
  // boot, increment counter. Otherwise reset.
  //
  // Problem: millis() resets each boot, so we can't use absolute time.
  // Solution: Use the boot number itself as a sequence detector.
  // If boots happen in quick succession, boot numbers will be consecutive.
  // We store the boot number when counter started, and check if current boot
  // is within a reasonable sequence.

  // Better approach: Store actual uptime when counter started using esp_timer
  // But esp_timer also resets. So we use a different method:
  // Store boot count and assume if user boots 3 times quickly, they're trying
  // to reset.

  // Simpler solution: Use the fact that NVS write time is ~few ms.
  // If 3 boots happen within 6 seconds total (wall clock time), counter works.
  // We'll store millis() of first reset, but that doesn't help across boots.

  // ACTUAL WORKING SOLUTION:
  // Store epoch timestamp using RTC time if available, or use a different
  // method. Since we don't have RTC at this point, we use boot counting with
  // timeout.

  // Let's use boot sequence numbers:
  uint32_t firstBoot = prefs.getUInt("enFirstBoot", 0);

  if (enCount == 0) {
    // First reset in sequence
    enCount = 1;
    firstBoot = currentBoot;
    prefs.putUInt("enFirstBoot", firstBoot);
    prefs.putUInt("enCount", enCount);
    Serial.printf("[EN] First reset detected, count=1, boot=%lu\n",
                  (unsigned long)currentBoot);
  } else {
    // Subsequent reset - check if it's within reasonable boot sequence
    // If too many boots have passed (e.g., >10 boots), assume timeout
    uint32_t bootDiff = currentBoot - firstBoot;

    if (bootDiff > 5) {
      // Too many boots, reset counter
      Serial.printf(
          "[EN] Boot sequence timeout (boots=%lu), resetting counter\n",
          (unsigned long)bootDiff);
      enCount = 1;
      firstBoot = currentBoot;
      prefs.putUInt("enFirstBoot", firstBoot);
      prefs.putUInt("enCount", enCount);
    } else {
      // Within sequence, increment
      enCount++;
      prefs.putUInt("enCount", enCount);
      Serial.printf("[EN] Reset %lu/%lu, boot=%lu\n", (unsigned long)enCount,
                    (unsigned long)EN_REQUIRED, (unsigned long)currentBoot);
    }
  }

  // Show LCD indicator AFTER LCD is initialized
  // We'll do this in setup() after initLCD()

  // Check if factory reset threshold reached
  if (enCount >= EN_REQUIRED) {
    Serial.println("[EN] ✓✓✓ TRIPLE RESET DETECTED!");
    if (lcdOk) {
      lcd2("RESET", "DONE");
      delay(1500);
    }
    factoryResetNow("EN triple reset");
  }
}

void showENResetIndicator() {
  uint32_t enCount = prefs.getUInt("enCount", 0);

  // Custom display logic:
  // 1/3 -> REFRESH
  // 2/3 -> (Skip/Hidden)
  // 3/3 -> Handled in main logic (RESET DONE)

  if (lcdOk) {
    if (enCount == 1) {
      Serial.println("[EN] Showing LCD: REFRESH");
      lcd2("REFRESH", "...");
      delay(1200);
    }
    // enCount == 2 is skipped (hidden)
  }
}

// =====================
// smartDelay (keeps web responsive)
// =====================
void smartDelay(uint32_t ms) {
  uint32_t t0 = millis();
  while (millis() - t0 < ms) {
    server.handleClient();
    if (apOn)
      dnsServer.processNextRequest();
    yield();
    delay(5);
  }
}

const char *wifiStatusStr(wl_status_t s) {
  switch (s) {
  case WL_NO_SSID_AVAIL:
    return "NO_SSID";
  case WL_CONNECTED:
    return "CONNECTED";
  case WL_CONNECT_FAILED:
    return "CONNECT_FAILED";
  case WL_CONNECTION_LOST:
    return "CONNECTION_LOST";
  case WL_DISCONNECTED:
    return "DISCONNECTED";
  case WL_IDLE_STATUS:
    return "IDLE";
  default:
    return "UNKNOWN";
  }
}

void logWifiStatus(const char *tag) {
  wl_status_t st = WiFi.status();
  int rssi = (st == WL_CONNECTED) ? WiFi.RSSI() : 0;
  Serial.printf("[%s] WiFi status=%s (%d), RSSI=%d dBm\n", tag,
                wifiStatusStr(st), (int)st, rssi);
}

uint32_t jitteredDelay(uint32_t baseMs, int32_t jitterMs = 100) {
  int32_t j = random(-jitterMs, jitterMs + 1);
  int32_t v = (int32_t)baseMs + j;
  if (v < 0)
    v = 0;
  return (uint32_t)v;
}

void renderIdleScreen() {
  if (!lcdOk)
    return;

  uint32_t now = millis();
  if (now - lastIdleDraw < IDLE_ANIM_MS)
    return;
  lastIdleDraw = now;

  // Use storeName if available (from server), fallback to storeId
  String displayName = (storeName.length() > 0) ? storeName : storeId;
  String msg = "SELAMAT DATANG DI " + displayName;

  // Simple character-based scrolling for LCD 16x2
  if (msg.length() <= 16) {
    // Message fits, just display centered
    lcd.clear();
    lcd.setCursor(0, 0);
    lcd.print(msg);
  } else {
    // Scroll message
    static int scrollPos = 0;
    lcd.clear();
    lcd.setCursor(0, 0);

    // Create scrolling substring
    String displayText = msg + "    "; // Add spaces for gap
    int totalLen = displayText.length();

    String scrollText = "";
    for (int i = 0; i < 16; i++) {
      scrollText += displayText.charAt((scrollPos + i) % totalLen);
    }

    lcd.print(scrollText);
    scrollPos++;
    if (scrollPos >= totalLen)
      scrollPos = 0;
  }

  // Show "READY" on line 2
  lcd.setCursor(0, 1);
  lcd.print("READY");
}

// =====================
// NTP for TLS
// =====================
bool syncTimeNtp(uint32_t maxWaitMs = 15000) {
  configTime(7 * 3600, 0, "pool.ntp.org", "time.google.com",
             "time.cloudflare.com");
  Serial.print("[NTP] syncing");
  uint32_t start = millis();
  time_t now = time(nullptr);

  while (now < 1700000000 && (millis() - start) < maxWaitMs) {
    smartDelay(500);
    Serial.print(".");
    now = time(nullptr);
  }
  Serial.println();

  if (now >= 1700000000) {
    Serial.print("[NTP] ok now=");
    Serial.println((long)now);
    timeSynced = true;
    return true;
  } else {
    Serial.print("[NTP] failed now=");
    Serial.println((long)now);
    timeSynced = false;
    return false;
  }
}

// =====================
// WiFi STA connect (ANTI-BADAI EDITION)
// =====================
bool connectWiFi(uint32_t timeoutMs = 15000) {
  if (wifiSsid.isEmpty())
    return false;

  // 1. Maximize TX Power (More range)
  WiFi.setTxPower(WIFI_POWER_19_5dBm);

  // 2. Disable Power Saving (No sleep!)
  WiFi.setSleep(false);

  // 3. Prefer strongest signal
  WiFi.setSortMethod(WIFI_CONNECT_AP_BY_SIGNAL);

  WiFi.begin(wifiSsid.c_str(), wifiPass.c_str());
  Serial.print("[WIFI] Connecting to ");
  Serial.println(wifiSsid);

  uint32_t t0 = millis();
  while (WiFi.status() != WL_CONNECTED && millis() - t0 < timeoutMs) {
    smartDelay(250);
    Serial.print(".");
  }
  Serial.println();

  if (WiFi.status() == WL_CONNECTED) {
    WiFi.setSleep(false); // Ensure it stays off
    WiFi.setAutoReconnect(true);

    Serial.println("[WIFI] sleep disabled, auto-reconnect enabled (MAX POWER)");
    Serial.print("[WIFI] OK IP: ");
    Serial.println(WiFi.localIP());

    logWifiStatus("CONNECTED");

    // Play "Wifi Tersambung" (0006.mp3)
    playTrack(6, 7000); // 7 seconds duration

    // Sync time and wait a bit for network stack to stabilize
    if (syncTimeNtp(12000)) {
      Serial.println("[WIFI] Waiting 2s for network stack stability...");
      delay(2000); // Give SSL stack time to initialize after NTP
    }
    return true;
  }

  Serial.println("[WIFI] Failed");
  logWifiStatus("FAIL");
  return false;
}

// =====================
// AP control + policy
// =====================
String apSsid() {
  uint64_t chip = ESP.getEfuseMac();
  char buf[32];
  snprintf(buf, sizeof(buf), "SOUNDBOX-%04X", (uint16_t)(chip & 0xFFFF));
  return String(buf);
}

void startAP() {
  if (apOn)
    return;

  WiFi.mode(WIFI_AP_STA);
  delay(50);

  String ssid = apSsid();
  WiFi.softAP(ssid.c_str());
  delay(50);

  IPAddress apIP = WiFi.softAPIP();
  dnsServer.start(DNS_PORT, "*", apIP);

  apOn = true;
  Serial.print("[AP] ON SSID: ");
  Serial.println(ssid);
  Serial.print("[AP] ON IP  : ");
  Serial.println(apIP);

  if (lcdOk)
    lcd2("SETUP", "192.168.4.1");
}

void stopAP() {
  if (!apOn)
    return;
  dnsServer.stop();
  WiFi.softAPdisconnect(true);
  apOn = false;
  Serial.println("[AP] OFF (ready)");
}

void enforceApPolicy() {
  if (WiFi.status() == WL_CONNECTED && isPaired())
    stopAP();
  else
    startAP();
}

// =====================
// Portal HTML
// =====================
String htmlEscape(String s) {
  s.replace("&", "&amp;");
  s.replace("<", "&lt;");
  s.replace(">", "&gt;");
  s.replace("\"", "&quot;");
  s.replace("'", "&#39;");
  return s;
}

String portalPage() {
  int n = WiFi.scanNetworks();
  String opt = "";
  for (int i = 0; i < n; i++) {
    String ssid = WiFi.SSID(i);
    int rssi = WiFi.RSSI(i);
    String strength = (rssi > -50)   ? "Sangat Baik"
                      : (rssi > -60) ? "Baik"
                      : (rssi > -70) ? "Cukup"
                                     : "Lemah";
    opt += "<option value='" + htmlEscape(ssid) + "'>" + htmlEscape(ssid) +
           " (" + strength + ")</option>";
  }
  if (n <= 0)
    opt = "<option value=''>Tidak ada jaringan</option>";

  String qrStatus = isPaired() ? ("✓ " + htmlEscape(storeId)) : "Belum Discan";
  String qrColor = isPaired() ? "#10b981" : "#ef4444";

  String wifiStatus = (WiFi.status() == WL_CONNECTED) ? ("✓ " + WiFi.SSID())
                                                      : "Belum Terhubung";
  String wifiColor = (WiFi.status() == WL_CONNECTED) ? "#10b981" : "#1f2937";

  String h = R"rawliteral(
<!DOCTYPE html>
<html lang="id">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>🔊 Setup WiFi Soundbox</title>
  <style>
    * {
      margin: 0;
      padding: 0;
      box-sizing: border-box;
    }
    body {
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
      background: linear-gradient(135deg, #0f2027 0%, #203a43 50%, #2c5364 100%);
      min-height: 100vh;
      padding: 20px;
      display: flex;
      align-items: center;
      justify-content: center;
    }
    .container {
      background: rgba(255, 255, 255, 0.95);
      backdrop-filter: blur(10px);
      border-radius: 20px;
      box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3);
      max-width: 500px;
      width: 100%;
      padding: 30px;
      animation: slideIn 0.5s ease-out;
    }
    @keyframes slideIn {
      from {
        opacity: 0;
        transform: translateY(-20px);
      }
      to {
        opacity: 1;
        transform: translateY(0);
      }
    }
    .header {
      text-align: center;
      margin-bottom: 30px;
    }
    .header h1 {
      font-size: 28px;
      background: linear-gradient(135deg, #2563eb 0%, #1e40af 100%);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
      background-clip: text;
      margin-bottom: 10px;
    }
    .status-grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 15px;
      margin-bottom: 25px;
    }
    .status-card {
      background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
      padding: 15px;
      border-radius: 12px;
      box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    .status-label {
      font-size: 12px;
      color: #6b7280;
      font-weight: 600;
      margin-bottom: 5px;
      text-transform: uppercase;
      letter-spacing: 0.5px;
    }
    .status-value {
      font-size: 16px;
      font-weight: 700;
      color: #1f2937;
    }
    .form-group {
      margin-bottom: 20px;
    }
    label {
      display: block;
      font-size: 14px;
      font-weight: 600;
      color: #374151;
      margin-bottom: 8px;
    }
    select, input {
      width: 100%;
      padding: 14px;
      border: 2px solid #e5e7eb;
      border-radius: 10px;
      font-size: 15px;
      transition: all 0.3s ease;
      background: white;
    }
    select:focus, input:focus {
      outline: none;
      border-color: #2563eb;
      box-shadow: 0 0 0 3px rgba(37, 99, 235, 0.1);
    }
    .password-wrapper {
      position: relative;
    }
    .password-wrapper input {
      padding-right: 50px;
    }
    .toggle-password {
      position: absolute;
      right: 12px;
      top: 50%;
      transform: translateY(-50%);
      background: none;
      border: none;
      cursor: pointer;
      padding: 5px;
      width: 35px;
      height: 35px;
      display: flex;
      align-items: center;
      justify-content: center;
      border-radius: 5px;
      transition: background 0.2s;
      color: #6b7280;
    }
    .toggle-password:hover {
      background: #f3f4f6;
      color: #2563eb;
    }
    .btn {
      width: 100%;
      padding: 16px;
      border: none;
      border-radius: 10px;
      font-size: 16px;
      font-weight: 700;
      cursor: pointer;
      transition: all 0.3s ease;
      text-transform: uppercase;
      letter-spacing: 0.5px;
    }
    .btn-primary {
      background: linear-gradient(135deg, #2563eb 0%, #1e40af 100%);
      color: white;
      box-shadow: 0 4px 15px rgba(37, 99, 235, 0.4);
    }
    .btn-primary:hover {
      transform: translateY(-2px);
      box-shadow: 0 6px 20px rgba(37, 99, 235, 0.5);
    }
    .btn-primary:active {
      transform: translateY(0);
    }
    .divider {
      height: 1px;
      background: linear-gradient(90deg, transparent, #e5e7eb, transparent);
      margin: 30px 0;
    }
    .qr-section {
      background: #f9fafb;
      padding: 25px;
      border-radius: 15px;
      margin-bottom: 20px;
    }
    .scanner-container {
      position: relative;
      width: 100%;
      max-width: 400px;
      margin: 0 auto 20px;
      border-radius: 15px;
      overflow: hidden;
      background: #000;
    }
    #qr-video {
      width: 100%;
      height: auto;
      display: block;
    }
    .scan-overlay {
      position: absolute;
      top: 0;
      left: 0;
      right: 0;
      bottom: 0;
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
    }
    .scan-box {
      width: 200px;
      height: 200px;
      border: 3px solid #2563eb;
      border-radius: 15px;
      box-shadow: 0 0 0 9999px rgba(0, 0, 0, 0.5);
      animation: pulse 2s infinite;
    }
    @keyframes pulse {
      0%, 100% {
        border-color: #2563eb;
      }
      50% {
        border-color: #10b981;
      }
    }
    .scan-text {
      color: white;
      margin-top: 20px;
      background: rgba(0, 0, 0, 0.7);
      padding: 8px 16px;
      border-radius: 8px;
      font-size: 14px;
      font-weight: 600;
    }
    .scanner-status {
      text-align: center;
    }
    .btn-scan {
      background: linear-gradient(135deg, #10b981 0%, #059669 100%);
      color: white;
      box-shadow: 0 4px 15px rgba(16, 185, 129, 0.4);
    }
    .btn-scan:hover {
      transform: translateY(-2px);
      box-shadow: 0 6px 20px rgba(16, 185, 129, 0.5);
    }
    .pairing-success {
      background: linear-gradient(135deg, #d1fae5 0%, #a7f3d0 100%);
      border-radius: 12px;
      margin-bottom: 15px;
    }
    @media (max-width: 600px) {
      .status-grid {
        grid-template-columns: 1fr;
      }
      .container {
        padding: 20px;
      }
      .scan-box {
        width: 150px;
        height: 150px;
      }
    }
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <h1>🔊 Setup WiFi Soundbox</h1>
      <p style="color: #6b7280; font-size: 14px;">Konfigurasi koneksi perangkat Anda</p>
    </div>

    <div class="status-grid">
      <div class="status-card">
        <div class="status-label">Status WiFi</div>
        <div class="status-value" style="color: )rawliteral" +
             wifiColor + R"rawliteral(;">)rawliteral" + wifiStatus +
             R"rawliteral(</div>
      </div>
      <div class="status-card">
        <div class="status-label">Status QR Code</div>
        <div class="status-value" style="color: )rawliteral" +
             qrColor + R"rawliteral(;">)rawliteral" + qrStatus +
             R"rawliteral(</div>
      </div>
    </div>

    <!-- QR Scanner Section -->
    <div id="qr-section" class="qr-section">
      <h3 style="font-size: 18px; color: #374151; margin-bottom: 15px; text-align: center;">
        📷 Langkah 1: Scan QR Code
      </h3>

      <div id="scanner-container" class="scanner-container" style="display: none;">
        <video id="qr-video" playsinline autoplay></video>
        <div class="scan-overlay">
          <div class="scan-box"></div>
          <p class="scan-text">Arahkan kamera ke QR code</p>
        </div>
      </div>

      <div id="scanner-status" class="scanner-status">
        <p style="color: #6b7280; margin-bottom: 15px; text-align: center;">
          Scan QR code dari admin panel
        </p>
        <button type="button" class="btn btn-scan" onclick="startQRScanner()">
          📸 Mulai Scan QR Code
        </button>
      </div>

      <div id="pairing-success" class="pairing-success" style="display: none;">
        <div style="text-align: center; padding: 20px;">
          <div style="font-size: 48px; margin-bottom: 10px;">✅</div>
          <h4 style="color: #10b981; margin-bottom: 10px;">QR Code Berhasil!</h4>
          <p style="color: #6b7280; font-size: 14px;" id="paired-store-name">Toko: -</p>
        </div>
      </div>
    </div>

    <div class="divider"></div>

    <!-- WiFi Setup Section -->
    <div id="wifi-section" class="wifi-section">
      <h3 style="font-size: 18px; color: #374151; margin-bottom: 15px; text-align: center;">
        📶 Langkah 2: Setup WiFi
      </h3>
    </div>

    <form method="POST" action="/save-wifi">
      <div class="form-group">
        <label for="ssid">📶 Pilih Jaringan WiFi</label>
        <select name="ssid" id="ssid" required>
          )rawliteral" +
             opt + R"rawliteral(
        </select>
      </div>
      
      <div class="form-group">
        <label for="pass">🔐 Password WiFi</label>
        <div class="password-wrapper">
          <input type="password" name="pass" id="pass" placeholder="Masukkan password">
          <button type="button" class="toggle-password" onclick="togglePassword()">
            <svg id="eye-icon" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"></path>
              <circle cx="12" cy="12" r="3"></circle>
            </svg>
          </button>
        </div>
      </div>

      <button type="submit" class="btn btn-primary">💾 Sambungkan WiFi</button>
    </form>
  </div>

  <script>
    let scannerActive = false;
    let videoStream = null;

    function togglePassword() {
      const passInput = document.getElementById('pass');
      const eyeIcon = document.getElementById('eye-icon');
      
      if (passInput.type === 'password') {
        passInput.type = 'text';
        eyeIcon.innerHTML = '<path d="M17.94 17.94A10.07 10.07 0 0 1 12 20c-7 0-11-8-11-8a18.45 18.45 0 0 1 5.06-5.94M9.9 4.24A9.12 9.12 0 0 1 12 4c7 0 11 8 11 8a18.5 18.5 0 0 1-2.16 3.19m-6.72-1.07a3 3 0 1 1-4.24-4.24"></path><line x1="1" y1="1" x2="23" y2="23"></line>';
      } else {
        passInput.type = 'password';
        eyeIcon.innerHTML = '<path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"></path><circle cx="12" cy="12" r="3"></circle>';
      }
    }

    async function startQRScanner() {
      const scannerStatus = document.getElementById('scanner-status');
      const scannerContainer = document.getElementById('scanner-container');
      const video = document.getElementById('qr-video');

      try {
        videoStream = await navigator.mediaDevices.getUserMedia({
          video: { facingMode: 'environment' }
        });

        video.srcObject = videoStream;
        scannerStatus.style.display = 'none';
        scannerContainer.style.display = 'block';
        scannerActive = true;

        // Note: Actual QR scanning would require jsQR library
        // For now, redirect to manual input or use external scanner
      } catch (error) {
        alert('Tidak dapat mengakses kamera.\n\nSilakan gunakan fitur scan QR dari aplikasi kamera ponsel Anda, lalu buka link yang muncul.');
        console.error('Camera error:', error);
      }
    }
  </script>
</body>
</html>
)rawliteral";

  return h;
}

// =====================
// HTTPS helper
// =====================
int httpsGetSimple(const String &url, String &outBody) {
  WiFiClientSecure client;
  client.setInsecure();

  HTTPClient http;
  http.setTimeout(HTTP_TIMEOUT_MS);
  if (!http.begin(client, url))
    return -1000;

  int code = http.GET();
  if (code > 0)
    outBody = http.getString();
  http.end();
  return code;
}

// =====================
// HTTPS Request Handlers
// =====================

void handleRoot() {
  Serial.println("[HTTP] Serving portal page...");
  Serial.printf("[HTTP] Free heap before: %d\n", ESP.getFreeHeap());

  // 1. WiFi scan
  int n = WiFi.scanNetworks();
  String opt = "";
  for (int i = 0; i < n; i++) {
    String ssid = WiFi.SSID(i);
    int rssi = WiFi.RSSI(i);
    String strength = (rssi > -50)   ? "Sangat Baik"
                      : (rssi > -60) ? "Baik"
                      : (rssi > -70) ? "Cukup"
                                     : "Lemah";
    opt += "<option value='" + htmlEscape(ssid) + "'>" + htmlEscape(ssid) +
           " (" + strength + ")</option>";
  }
  if (n <= 0)
    opt = "<option value=''>Tidak ada jaringan</option>";

  String pairStatus =
      isPaired() ? ("✓ " + htmlEscape(storeId)) : "Belum Paired";
  String pairColor = isPaired() ? "#10b981" : "#ef4444";
  String wifiStatus = (WiFi.status() == WL_CONNECTED) ? ("✓ " + WiFi.SSID())
                                                      : "Belum Terhubung";
  String wifiColor = (WiFi.status() == WL_CONNECTED) ? "#10b981" : "#1f2937";
  String devId = apSsid(); // Device ID for pairing

  // 2. Send HTML in chunks
  server.setContentLength(CONTENT_LENGTH_UNKNOWN);
  server.send(200, "text/html", "");

  server.sendContent(R"rawliteral(<!DOCTYPE html>
<html lang="id">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Setup WiFi Soundbox</title>
  <style>
    *{margin:0;padding:0;box-sizing:border-box}
    body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif;background:linear-gradient(135deg,#0f2027,#203a43,#2c5364);min-height:100vh;padding:20px;display:flex;align-items:center;justify-content:center}
    .container{background:rgba(255,255,255,0.95);border-radius:20px;box-shadow:0 20px 60px rgba(0,0,0,0.3);max-width:500px;width:100%;padding:30px}
    .header{text-align:center;margin-bottom:30px}
    .header h1{font-size:28px;background:linear-gradient(135deg,#2563eb,#1e40af);-webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text;margin-bottom:10px}
    .status-grid{display:grid;grid-template-columns:1fr 1fr;gap:15px;margin-bottom:25px}
    .status-card{background:linear-gradient(135deg,#f5f7fa,#c3cfe2);padding:15px;border-radius:12px;box-shadow:0 4px 6px rgba(0,0,0,0.1)}
    .status-label{font-size:12px;color:#6b7280;font-weight:600;margin-bottom:5px;text-transform:uppercase;letter-spacing:0.5px}
    .status-value{font-size:16px;font-weight:700;color:#1f2937}
    .form-group{margin-bottom:20px}
    label{display:block;font-size:14px;font-weight:600;color:#374151;margin-bottom:8px}
    select,input{width:100%;padding:14px;border:2px solid #e5e7eb;border-radius:10px;font-size:15px;background:white}
    select:focus,input:focus{outline:none;border-color:#2563eb;box-shadow:0 0 0 3px rgba(37,99,235,0.1)}
    .btn{width:100%;padding:16px;border:none;border-radius:10px;font-size:16px;font-weight:700;cursor:pointer;text-transform:uppercase;letter-spacing:0.5px}
    .btn-primary{background:linear-gradient(135deg,#2563eb,#1e40af);color:white;box-shadow:0 4px 15px rgba(37,99,235,0.4)}
    .divider{height:1px;background:linear-gradient(90deg,transparent,#e5e7eb,transparent);margin:30px 0}
    .device-id{font-size:24px;font-weight:800;letter-spacing:2px;color:#1e40af;background:#eff6ff;padding:15px;border-radius:10px;border:2px dashed #93c5fd}
  </style>
</head>
<body>
<div class="container">
  <div class="header">
    <h1>Setup Soundbox</h1>
    <p style="color:#6b7280">Konfigurasi WiFi</p>
  </div>
  <div class="status-grid">
    <div class="status-card">
      <div class="status-label">WiFi</div>
      <div class="status-value" style="color:)rawliteral");

  server.sendContent(wifiColor + ";\">");
  server.sendContent(wifiStatus);

  server.sendContent(R"rawliteral(</div>
    </div>
    <div class="status-card">
      <div class="status-label">Pairing</div>
      <div class="status-value" style="color:)rawliteral");

  server.sendContent(pairColor + ";\">");
  server.sendContent(pairStatus);

  server.sendContent(R"rawliteral(</div>
    </div>
  </div>
  <div style="text-align:center;margin-bottom:25px">
    <div class="status-label">Device ID (untuk pairing di Admin)</div>
    <div class="device-id">)rawliteral");

  server.sendContent(devId);

  server.sendContent(R"rawliteral(</div>
  </div>
  <form action="/save-wifi" method="POST">
    <div class="form-group">
      <label>Pilih Jaringan WiFi</label>
      <select name="ssid">)rawliteral");

  server.sendContent(opt);

  server.sendContent(R"rawliteral(</select>
    </div>
    <div class="form-group">
      <label>Password WiFi</label>
      <input type="password" name="pass" placeholder="Masukkan password WiFi">
    </div>
    <button type="submit" class="btn btn-primary">Simpan & Restart</button>
  </form>
  <div class="divider"></div>
  <div style="text-align:center;color:#9ca3af;font-size:12px">
    <p>SOUNDBOX QRIS</p>
    <p style="margin-top:5px">Heap: )rawliteral");

  server.sendContent(String(ESP.getFreeHeap()));

  server.sendContent(R"rawliteral( bytes</p>
  </div>
</div>
</body>
</html>)rawliteral");

  server.sendContent(""); // End chunked transfer
  Serial.printf("[HTTP] Free heap after: %d\n", ESP.getFreeHeap());
  Serial.println("[HTTP] Portal page sent OK");
}

void handleSaveWifi() {
  String ssid = server.arg("ssid");
  String pass = server.arg("pass");

  if (ssid.length() > 0) {
    prefs.putString("wifiSsid", ssid);
    prefs.putString("wifiPass", pass);

    server.send(
        200, "text/html",
        "<html><body><h1>Simpan Berhasil!</h1><p>SSID: " + ssid +
            "</p><p>Password: ***</p><p>Device akan restart dalam 3 "
            "detik...</p><script>setTimeout(function(){window.location.href='/"
            "restart';}, 3000);</script></body></html>");

    delay(2000);
    ESP.restart();
  } else {
    server.send(400, "text/plain", "Missing SSID");
  }
}

void handlePair() {
  String sid = server.arg("store_id");
  String tok = server.arg("token");
  String nameParam = server.arg("name");

  if (sid.length() > 0 && tok.length() > 0) {
    storeId = sid;
    deviceToken = tok;

    if (nameParam.length() > 0) {
      storeName = urlDecode(nameParam);
      prefs.putString("storeName", storeName);
    }

    prefs.putString("storeId", storeId);
    prefs.putString("deviceToken", deviceToken);

    Serial.printf("[PAIR] Saved store_id=%s, token=%s\n", storeId.c_str(),
                  deviceToken.c_str());

    if (dfOk)
      playTrack(7, 4000); // "Pairing Berhasil"

    server.send(
        200, "text/html",
        "<html><head><meta charset='UTF-8'><meta name='viewport' "
        "content='width=device-width, initial-scale=1.0'></head><body><div "
        "style='text-align:center; padding:50px; "
        "font-family:sans-serif;'><h1>Pairing Berhasil!</h1><p>Store ID: " +
            storeId +
            "</p><p>Silakan tutup halaman ini.</p></div></body></html>");
  } else {
    server.send(400, "text/plain", "Missing store_id or token");
  }
}

void handleStatus() {
  StaticJsonDocument<512> doc;
  doc["uptime"] = millis() / 1000;
  doc["wifi_status"] =
      (WiFi.status() == WL_CONNECTED) ? "CONNECTED" : "DISCONNECTED";
  doc["wifi_ssid"] = WiFi.SSID();
  doc["ip"] = WiFi.localIP().toString();
  doc["rssi"] = WiFi.RSSI();
  doc["store_id"] = storeId;
  doc["store_name"] = storeName;
  doc["device_id"] = apSsid();
  doc["paired"] = isPaired();
  doc["time_synced"] = timeSynced;
  doc["heap_free"] = ESP.getFreeHeap();
  doc["poll_fail_streak"] = pollFailStreak;

  String output;
  serializeJson(doc, output);
  server.send(200, "application/json", output);
}

void handleTestWorker() {
  PollResult result = pollOnce();

  if (result == POLL_GOT_TX) {
    server.send(200, "text/plain", "PASS: Transaction found and played");
  } else if (result == POLL_NO_TX) {
    server.send(200, "text/plain",
                "PASS: Worker reachable, no pending transaction");
  } else {
    server.send(500, "text/plain",
                "FAIL: Error reaching worker (check Serial logs)");
  }
}

void handleFactory() {
  server.send(200, "text/plain", "Factory Resetting... Goodbye!");
  delay(1000);
  prefs.clear();
  ESP.restart();
}

void handleRestart() {
  server.send(200, "text/plain", "Restarting...");
  delay(1000);
  ESP.restart();
}

void handleHeap() { server.send(200, "text/plain", String(ESP.getFreeHeap())); }

void handleNotFound() {
  // Captive Portal Redirection
  if (server.hostHeader() != "192.168.4.1") {
    server.sendHeader("Location", "http://192.168.4.1/");
    server.send(302);
    return;
  }

  server.send(404, "text/plain", "404 Not Found");
}

// Helper for URL decoding
String urlDecode(String str) {
  String ret;
  char c;
  int i, len = str.length();

  for (i = 0; i < len; i++) {
    c = str[i];
    if (c == '+') {
      ret += ' ';
    } else if (c == '%') {
      char code0 = str[i + 1];
      char code1 = str[i + 2];
      c = (hexCharToInt(code0) << 4) | hexCharToInt(code1);
      ret += c;
      i += 2;
    } else {
      ret += c;
    }
  }
  return ret;
}

int hexCharToInt(char c) {
  if (c >= '0' && c <= '9')
    return c - '0';
  if (c >= 'A' && c <= 'F')
    return c - 'A' + 10;
  if (c >= 'a' && c <= 'f')
    return c - 'a' + 10;
  return 0;
}

// =====================
// DFPlayer Audio Logic (Updated)
// =====================
void dfInitNonBlocking() {
  DFSerial.begin(9600, SERIAL_8N1, DF_RX, DF_TX);
  smartDelay(1500); // Increased delay for stability (was 250)

  dfOk = df.begin(DFSerial);
  if (dfOk) {
    df.volume(25); // Lower volume to 25 to prevent brownout
    Serial.println("[DF] ✓ Initialized");

    // DEBUG: Check file count
    int fileCount = df.readFileCounts();
    Serial.printf("[DF] Files found on SD: %d\n", fileCount);

  } else {
    Serial.println("[DF] ✗ Init failed (audio disabled)");
  }
}

void playTrack(int t, int ms) {
  if (!dfOk)
    return;
  Serial.printf("[DF] Play (Folder MP3) track %d (%d ms)\n", t, ms);

  // Use playMp3Folder for reliable ordering
  // Files MUST be in folder "MP3" and named "0001.mp3", etc.
  df.playMp3Folder(t);

  smartDelay(ms);
}

// Logic Terbilang Recursive
void playTerbilang(long n) {
  if (!dfOk)
    return;

  // Mapping based on user request:
  // 10=Nol, 11=Satu .. 19=Sembilan
  // 20=Sepuluh, 21=Sebelas, 22=Belas
  // 23=Puluh, 24=Ratus, 25=Ribu, 26=Juta
  // 27=Seratus, 28=Seribu

  // Delays tailored to user files (2s phrases, 1s numbers) + buffer

  if (n < 12) {
    // 0..11 matches mapping directly (0->10, 1->11..9->19, 10->20, 11->21)
    playTrack(10 + n, 1200);
  } else if (n < 20) {
    // 12..19 -> "Dua Belas" .. "Sembilan Belas"
    playTerbilang(n - 10); // e.g. 12 -> 2 ("Dua")
    playTrack(22, 1200);   // "Belas"
  } else if (n < 100) {
    // 20..99 -> "Dua Puluh" .. "Sembilan Puluh Sembilan"
    playTerbilang(n / 10);
    playTrack(23, 1200); // "Puluh"
    if (n % 10 > 0)
      playTerbilang(n % 10);
  } else if (n < 200) {
    // 100..199 -> "Seratus" ...
    playTrack(27, 1200); // "Seratus"
    if (n % 100 > 0)
      playTerbilang(n % 100);
  } else if (n < 1000) {
    // 200..999 -> "Dua Ratus" ...
    playTerbilang(n / 100);
    playTrack(24, 1200); // "Ratus"
    if (n % 100 > 0)
      playTerbilang(n % 100);
  } else if (n < 2000) {
    // 1000..1999 -> "Seribu" ...
    playTrack(28, 1200); // "Seribu"
    if (n % 1000 > 0)
      playTerbilang(n % 1000);
  } else if (n < 1000000) {
    // 2000..999,999 -> "Dua Ribu" ...
    playTerbilang(n / 1000);
    playTrack(25, 1200); // "Ribu"
    if (n % 1000 > 0)
      playTerbilang(n % 1000);
  } else if (n < 1000000000) {
    // 1M.. -> "Satu Juta" ...
    playTerbilang(n / 1000000);
    playTrack(26, 1200); // "Juta"
    if (n % 1000000 > 0)
      playTerbilang(n % 1000000);
  }
}

// Update LCD + Audio simultaneously
void playPaymentSequence(int amount) {
  if (!dfOk)
    return;

  Serial.println("[DF] Playing payment sequence...");

  // 0. "Kringggg" (0005.mp3) + Show "UANG MASUK"
  if (lcdOk)
    lcd2("UANG", "MASUK");
  playTrack(5, 1500);

  // 1. "Uang Masuk" (2s)
  // LCD already showing "UANG MASUK"
  playTrack(1, 2200);

  // 2. "Sebesar" (2s) + Show "SEBESAR"
  if (lcdOk)
    lcd2("SEBESAR", "");
  playTrack(2, 2200);

  // 3. Amount (Terbilang) (1s each) + Show "RP xxx"
  if (lcdOk)
    lcd2("RP" + String(amount), "");

  if (amount == 0) {
    playTrack(10, 1200); // "Nol"
  } else {
    playTerbilang(amount);
  }

  // 4. "Rupiah" (2s)
  // Keep showing amount
  playTrack(3, 2200);

  // 5. "Terima Kasih" (Asumsi 3s aman) + Show "TERIMA KASIH"
  if (lcdOk)
    lcd2("TERIMA", "KASIH");
  playTrack(4, 3000);
}

// =====================
// LCD payment screens
// =====================
void showPaymentScreens(int amount) {
  if (!lcdOk)
    return;

  Serial.println("[PAY] Showing payment animation...");

  lcd2("UANG", "MASUK");
  smartDelay(LCD_STEP_MS);

  lcd2("SEBESAR", "");
  smartDelay(LCD_STEP_MS);

  lcd2("RP" + String(amount), "");
  smartDelay(LCD_AMOUNT_MS);

  lcd2("MAKASIH!!", "");
  smartDelay(2000);

  Serial.println("[PAY] Animation complete");
}

// =====================
// Poll Worker
// =====================
PollResult pollOnce() {
  if (WiFi.status() != WL_CONNECTED) {
    Serial.println("[POLL] ✗ WiFi not connected");
    logWifiStatus("POLL");
    return POLL_ERROR;
  }
  if (!isPaired()) {
    Serial.println("[POLL] ✗ Device not paired");
    return POLL_ERROR;
  }

  if (!timeSynced) {
    Serial.println("[POLL] NTP not synced, syncing...");
    if (syncTimeNtp(12000)) {
      Serial.println("[POLL] NTP synced, waiting for SSL stack...");
      delay(2000); // Wait for SSL to be ready after NTP
    }
  }

  // Check heap before HTTPS call
  uint32_t freeHeap = ESP.getFreeHeap();
  Serial.printf("[POLL] Free heap: %lu bytes\n", (unsigned long)freeHeap);

  if (freeHeap < 20000) {
    Serial.println("[POLL] ⚠ Low memory, forcing GC...");
    delay(100);
  }

  WiFiClientSecure client;
  client.setInsecure();
  client.setHandshakeTimeout(15); // 15 second SSL handshake timeout

  HTTPClient http;
  http.setTimeout(HTTP_TIMEOUT_MS);
  http.setReuse(false); // Don't reuse connections

  String url = String(WORKER_BASE) + "/next-transaction?store_id=" + storeId;

  Serial.printf("[POLL] Connecting to %s\n", WORKER_BASE);

  if (!http.begin(client, url)) {
    Serial.println("[POLL] ✗ http.begin failed");
    return POLL_ERROR;
  }

  http.addHeader("x-device-token", deviceToken);

  int code = http.GET();

  // Retry once if connection failed
  if (code <= 0) {
    Serial.printf("[POLL] ✗ GET failed (code=%d: %s), retrying...\n", code,
                  http.errorToString(code).c_str());
    logWifiStatus("POLL");
    http.end();
    delay(300); // Brief delay before retry

    // Retry with fresh connection
    if (http.begin(client, url)) {
      http.addHeader("x-device-token", deviceToken);
      code = http.GET();

      if (code <= 0) {
        Serial.printf("[POLL] ✗ Retry failed, code=%d: %s\n", code,
                      http.errorToString(code).c_str());
        logWifiStatus("POLL");
        http.end();
        delay(100); // Cleanup delay
        return POLL_ERROR;
      }
    } else {
      Serial.println("[POLL] ✗ Retry http.begin failed");
      return POLL_ERROR;
    }
  }

  String body = http.getString();
  http.end();
  delay(100); // Cleanup delay after http.end()

  if (code != 200) {
    Serial.printf("[POLL] ✗ HTTP %d, body=%s\n", code, body.c_str());
    return POLL_ERROR;
  }

  // DEBUG: Print Body to see what we really got
  Serial.printf("[POLL] Body: %s\n", body.c_str());

  StaticJsonDocument<512> doc; // Increased buffer for safety
  DeserializationError jsonErr = deserializeJson(doc, body);
  if (jsonErr) {
    Serial.printf("[POLL] ✗ JSON parse error: %s\n", jsonErr.c_str());
    return POLL_ERROR;
  }

  // Update store name if provided
  String sName = doc["store_name"] | "";
  if (sName.length() > 0 && !sName.equals(storeName)) {
    Serial.printf("[POLL] New Name Detected: '%s' (was '%s')\n", sName.c_str(),
                  storeName.c_str());
    storeName = sName;
    prefs.putString("storeName", storeName);

    // Force immediate UI update if idle
    if (WiFi.status() == WL_CONNECTED && isPaired() && lcdOk) {
      renderIdleScreen();
    }
  }

  int amount = doc["amount"] | 0;
  const char *txid = doc["transaction_id"] | "";
  bool available = doc["available"] | false;

  // Fix: Check if transaction exists based on ID or Amount, even if
  // 'available' is false/missing
  if (!available && (strlen(txid) == 0 || amount <= 0)) {
    Serial.println("[POLL] OK (No TX)"); // Show heartbeat is working
    return POLL_NO_TX;
  }

  Serial.printf("[POLL] ✓ TX found: %s, amount=Rp%d\n", txid, amount);

  // showPaymentScreens(amount); // <-- DISABLED: Now unified inside
  // playPaymentSequence
  playPaymentSequence(amount);

  if (lcdOk)
    lcd2("READY", storeId);
  Serial.println("[POLL] Back to READY state");

  return POLL_GOT_TX;
}

// =====================
// Setup
// =====================
void setup() {
  Serial.begin(115200);
  delay(300);
  randomSeed(micros());

  Serial.println("\n╔════════════════════════════════════════╗");
  Serial.println("║  SOUNDBOX v2.1 - FLASH EN RESET       ║");
  Serial.println("╚════════════════════════════════════════╝");
  Serial.println();

  // Preferences MUST be opened BEFORE handleENTripleResetEarly
  Serial.println("[STEP] Opening preferences...");
  prefs.begin("soundbox", false);

  // LCD init BEFORE showing reset indicator
  Serial.println("[STEP] Initializing LCD...");
  lcdOk = initLCD();

  // Now handle EN reset detection (after prefs + LCD ready)
  Serial.println("[STEP] Checking EN reset sequence...");
  handleENTripleResetEarly();
  showENResetIndicator(); // Show "RESET X/3" if applicable

  // Load rest of preferences
  loadPrefs();
  Serial.printf("[STEP] ✓ SSID='%s', paired=%s\n", wifiSsid.c_str(),
                isPaired() ? "YES" : "NO");

  // DFPlayer init
  Serial.println("[STEP] Initializing DFPlayer...");
  dfInitNonBlocking();

  // Play "Selamat Datang" (0008.mp3)
  if (dfOk) {
    playTrack(8, 8000); // 8 seconds duration
  }

  // Start AP first (for lwIP init before server.begin)
  Serial.println("[STEP] Starting Access Point...");
  startAP();

  // =====================
  // HTTP Server Setup
  // =====================
  Serial.println("[STEP] Starting HTTP Server...");

  // Register Routes
  server.on("/", HTTP_GET, handleRoot);
  server.on("/save-wifi", HTTP_POST, handleSaveWifi);
  server.on("/pair", HTTP_GET, handlePair);
  server.on("/status", HTTP_GET, handleStatus);
  server.on("/test-worker", HTTP_GET, handleTestWorker);
  server.on("/factory", HTTP_GET, handleFactory);
  server.on("/restart", HTTP_GET, handleRestart);
  server.on("/heap", HTTP_GET, handleHeap);
  server.onNotFound(handleNotFound);

  server.begin();
  Serial.println("[HTTP] \u2713 Server started on port 80");

  // Connect to WiFi if configured
  if (!wifiSsid.isEmpty()) {
    Serial.println("[STEP] Connecting to WiFi...");
    connectWiFi(15000); // Plays Track 6 (Wifi Tersambung) inside

    // Fix: Play "Pairing Tersambung" (0007.mp3) if already paired
    if (WiFi.status() == WL_CONNECTED && isPaired()) {
      Serial.println("[STEP] Device is paired, playing pairing sound...");
      playTrack(7, 4000); // 4 seconds duration
    }
  } else {
    Serial.println("[STEP] No WiFi configured, skipping...");
  }

  // Enforce AP policy
  Serial.println("[STEP] Enforcing AP policy...");
  enforceApPolicy();

  // Set initial UI state
  String devId = apSsid();
  if (WiFi.status() == WL_CONNECTED && isPaired()) {
    if (lcdOk)
      lcd2("READY", storeId);
    Serial.println("[UI] Device READY");
  } else if (WiFi.status() == WL_CONNECTED) {
    if (lcdOk)
      lcd2("MENUNGGU..", devId);
    Serial.println("[UI] WiFi OK, waiting for pairing via worker");
  } else {
    if (lcdOk)
      lcd2("SET WIFI", "192.168.4.1");
    Serial.println("[UI] Waiting for WiFi setup");
  }

  Serial.println();
  Serial.println("╔════════════════════════════════════════╗");
  Serial.println("║           SETUP COMPLETE               ║");
  Serial.println("╚════════════════════════════════════════╝");
  Serial.println("\n📱 Setup Portal: http://192.168.4.1/");
  Serial.println("📊 Status: http://192.168.4.1/status");
  Serial.printf("🆔 Device ID: %s\n", devId.c_str());
  Serial.println("🗑️ Factory Reset: Press EN 3x OR http://192.168.4.1/factory");
  Serial.println();

  nextPollAt = millis() + jitteredDelay(POLL_MS);
  backoffMs = POLL_MS;

  // Clear EN reset counter after successful boot
  // Prevents accidental factory reset on normal reboots
  prefs.putUInt("enCount", 0);
  prefs.putUInt("enFirstBoot", 0);
}

// =====================
// Poll pairing from worker
// =====================
uint32_t nextPairPollAt = 0;

void pollPairing() {
  String devId = apSsid();
  String url = String(WORKER_BASE) + "/pair-check?device_id=" + devId;

  Serial.printf("[PAIR] Checking worker for pending pair (device_id=%s)...\n",
                devId.c_str());

  WiFiClientSecure client;
  client.setInsecure();
  HTTPClient http;
  http.setTimeout(HTTP_TIMEOUT_MS);

  if (!http.begin(client, url)) {
    Serial.println("[PAIR] HTTP begin failed");
    return;
  }

  int code = http.GET();
  if (code != 200) {
    Serial.printf("[PAIR] HTTP %d\n", code);
    http.end();
    return;
  }

  String body = http.getString();
  http.end();

  StaticJsonDocument<512> doc;
  if (deserializeJson(doc, body)) {
    Serial.println("[PAIR] JSON parse error");
    return;
  }

  bool paired = doc["paired"] | false;
  if (!paired) {
    Serial.println("[PAIR] No pending pair");
    return;
  }

  // Got pairing data!
  storeId = String((const char *)(doc["store_id"] | ""));
  deviceToken = String((const char *)(doc["device_token"] | ""));
  storeName = String((const char *)(doc["store_name"] | ""));

  if (storeId.isEmpty() || deviceToken.isEmpty()) {
    Serial.println("[PAIR] Empty store_id or device_token in response");
    return;
  }

  prefs.putString("storeId", storeId);
  prefs.putString("deviceToken", deviceToken);
  if (!storeName.isEmpty())
    prefs.putString("storeName", storeName);

  Serial.printf("[PAIR] \u2713 AUTO-PAIRED! store_id=%s, name=%s\n",
                storeId.c_str(), storeName.c_str());

  if (dfOk)
    playTrack(7, 4000); // "Pairing Berhasil"

  if (lcdOk)
    lcd2("TERSAMBUNG!", storeId);

  // Turn off AP now that we're paired
  enforceApPolicy();
}

// =====================
// Main Loop
// =====================
void loop() {
  // 1. Process HTTP server requests
  server.handleClient();

  // 2. Process DNS requests (for captive portal)
  if (apOn)
    dnsServer.processNextRequest();

  // 3. LCD Idle Animation
  unsigned long now = millis();
  if (now - lastIdleDraw > IDLE_ANIM_MS) {
    if (lcdOk)
      renderIdleScreen();
    lastIdleDraw = now;
  }

  // 4. Poll Worker for transactions (only if connected + paired)
  if (WiFi.status() == WL_CONNECTED && isPaired()) {
    if (now >= nextPollAt) {
      Serial.println("[LOOP] Polling worker...");
      PollResult res = pollOnce();

      if (res == POLL_ERROR) {
        pollFailStreak++;
        backoffMs = min(backoffMs * 2, MAX_BACKOFF);
        Serial.printf("[LOOP] Poll failed (streak=%d), backing off %d ms\n",
                      pollFailStreak, backoffMs);
      } else {
        pollFailStreak = 0;
        backoffMs = POLL_MS;
      }

      nextPollAt = now + jitteredDelay(backoffMs);
    }
  } else if (WiFi.status() == WL_CONNECTED && !isPaired()) {
    // 5. Poll Worker for pairing (WiFi OK but not yet paired)
    if (now >= nextPairPollAt) {
      pollPairing();
      nextPairPollAt = now + 10000; // Check every 10 seconds
    }
  } else {
    // 6. Auto-reconnect WiFi if dropped
    static uint32_t lastReconnectTry = 0;
    if (!wifiSsid.isEmpty() && WiFi.status() != WL_CONNECTED) {
      if (now - lastReconnectTry > 10000) {
        lastReconnectTry = now;
        Serial.println("[WIFI] Connection lost, reconnecting...");

        if (lcdOk)
          lcd2("WIFI", "RECON...");

        connectWiFi(8000);
        enforceApPolicy();

        if (WiFi.status() == WL_CONNECTED && isPaired() && lcdOk) {
          lcd2("READY", storeId);
        }
      }
    }
  }

  // Yield to allow background tasks
  delay(10);
}
