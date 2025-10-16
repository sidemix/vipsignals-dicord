# discord_sender.py
import os, requests, math

WEBHOOK = os.environ["DISCORD_WEBHOOK_URL"]
BRAND  = os.getenv("SIGNAL_TITLE", "⭐  VIP Signal  ⭐")

# simple smart decimals so PEPE etc. look nice
def fmt_price(x: float) -> str:
    x = float(x)
    if x == 0 or math.isnan(x) or math.isinf(x):
        return "0"
    absx = abs(x)
    if absx < 0.0001:  # very tiny
        return f"{x:.8f}"
    if absx < 0.01:
        return f"{x:.7f}".rstrip("0").rstrip(".")
    if absx < 1:
        return f"{x:.6f}".rstrip("0").rstrip(".")
    if absx < 10:
        return f"{x:.4f}".rstrip("0").rstrip(".")
    if absx < 1000:
        return f"{x:.3f}".rstrip("0").rstrip(".")
    return f"{x:.2f}".rstrip("0").rstrip(".")

NUM_EMO = ["1️⃣","2️⃣","3️⃣","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]

def currency_from_symbol(symbol: str) -> str:
    # "ENA/USDT" -> "USDT"; "BTC/USD" -> "USD"; default last leg
    try:
        return symbol.split("/")[-1].split(":")[0]
    except Exception:
        return "USDT"

def build_description(symbol, side, lev, eh, el, sl, tps, extras=None):
    cur = currency_from_symbol(symbol)
    dot = "🟢 **Long**" if side.upper() == "LONG" else "🔴 **Short**"

    # Ensure formatting (accept floats or preformatted strings)
    eh_s = eh if isinstance(eh, str) else fmt_price(eh)
    el_s = el if isinstance(el, str) else fmt_price(el)
    sl_s = sl if isinstance(sl, str) else fmt_price(sl)
    tps_s = [(tp if isinstance(tp, str) else fmt_price(tp)) for tp in tps]

    lines = []
    lines.append(f"{dot}\n")
    lines.append(f"**Name:** {symbol}")
    lines.append(f"**Leverage:** Cross ({int(lev)}x)\n")
    lines.append(f"🌀 **Entry Price ({cur})**: {el_s} – {eh_s}")
    lines.append(f"\n🎯 **Targets in {cur}:**")
    for i, tp in enumerate(tps_s[:10]):
        n = NUM_EMO[i] if i < len(NUM_EMO) else f"{i+1}."
        lines.append(f"{n} {tp}")
    lines.append(f"\n🛑 **StopLoss:** {sl_s}")

    if extras:
        # append on separate lines (e.g., TF)
        for k, v in extras.items():
            lines.append(f"\n- {k}: {v}")

    return "\n".join(lines)

def embed_color(side: str) -> int:
    return 0x00C853 if side.upper() == "LONG" else 0xD32F2F  # green/red

def send_signal_embed(symbol, side, lev, eh, el, sl, tps, extras=None):
    desc = build_description(symbol, side, lev, eh, el, sl, tps, extras)
    payload = {
        "embeds": [{
            "title": BRAND,
            "description": desc,
            "color": embed_color(side)
        }]
    }
    r = requests.post(WEBHOOK, json=payload, timeout=10)
    r.raise_for_status()

def send_info(msg: str):
    payload = {"embeds": [{"title": "Signals Bot", "description": msg}]}
    r = requests.post(WEBHOOK, json=payload, timeout=10)
    r.raise_for_status()
