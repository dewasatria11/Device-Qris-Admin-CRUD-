#ifndef CERTS_H
#define CERTS_H

// ==========================================
// SSL TRUST ANCHORS FOR SIM900A (BearSSL)
// ==========================================
//
// Karena SIM900A tidak support HTTPS native, kita pakai library SSLClient
// (Software SSL). Library ini butuh "Trust Anchors" (Sertifikat) dalam format
// binary BearSSL, BUKAN PEM string biasa.
//
// CARA GENERATE:
// 1. Download tool: https://github.com/OPEnSLab-OSU/SSLClient/tree/master/tools
// 2. Jalankan: python pycert_bearssl.py download google.com
// 3. Copy isi outputnya ke bawah ini.
//
// Untuk sementara, kita pakai Dummy/Empty anchor (UNSAFE - Bisa gagal verify).
// SANGAT DISARANKAN generate sendiri untuk "server.soundboxqris123.workers.dev"
//
// ==========================================

#include <SSLClient.h>

// Contoh struktur (Placeholder):
// Change this with output from pycert_bearssl.py

static const unsigned char TAs_DN[] = {0};
static const unsigned char TAs_RSA_N[] = {0};
static const unsigned char TAs_RSA_E[] = {0};

// Minimal empty anchor to compile
static const br_x509_trust_anchor TAs[] = {
    // Empty - validation will likely fail unless SSLClient is in insecure mode
};

static const size_t TAs_NUM = 0; // Set to 0 to try insecure mode?

#endif
