# Polymarket Arbitrage Bot

Bot Python untuk mendeteksi dan mengeksekusi peluang **Yes/No arbitrage** di Polymarket secara otomatis.

## Strategi

Beli YES + NO di market yang sama ketika combined ask price < 1.0.

**Guaranteed profit** = 1.0 - (harga_YES + harga_NO)

## Requirements

- Python 3.9+
- Wallet Polygon dengan USDC

## Instalasi

```bash
pip install py-clob-client web3 tenacity python-dotenv
```

## Setup

Buat file `.env` di root folder:

```
PRIVATE_KEY=0x_private_key_polygon_wallet_kamu
```

## Cara Pakai

**1. Dry run (simulasi, aman):**
```bash
python polymarket_arb_bot.py
```

**2. Live trading:**
Ubah `DRY_RUN` jadi `False` di bagian `CONFIG` dalam file bot.

## Konfigurasi

| Parameter | Default | Keterangan |
|---|---|---|
| `MIN_PROFIT_THRESHOLD` | 0.01 (1%) | Minimum profit sebelum eksekusi |
| `MAX_POSITION_SIZE` | 10.0 USDC | Max USDC per sisi trade |
| `MIN_LIQUIDITY` | 1000 USDC | Min likuiditas market |
| `SCAN_INTERVAL` | 15 detik | Interval scan market |
| `DRY_RUN` | True | Mode simulasi |
| `MAX_CONSECUTIVE_LOSSES` | 3 | Circuit breaker |

## Fitur

- Scan semua market aktif setiap 15 detik
- Deteksi peluang YES/NO arbitrage secara real-time
- Auto-eksekusi dengan Fill-or-Kill orders
- Partial fill protection (cancel YES kalau NO gagal)
- Circuit breaker (pause 5 menit setelah 3 losses berturut-turut)
- Logging ke console dan file `arb_bot.log`

## Disclaimer

Bot ini untuk tujuan edukasi. Trading prediction markets mengandung risiko. Gunakan dengan bijak.
