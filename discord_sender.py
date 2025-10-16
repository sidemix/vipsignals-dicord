import os, requests

WEBHOOK = os.environ["DISCORD_WEBHOOK_URL"]

def send_signal_embed(symbol, side, lev, eh, el, sl, tps, extras=None):
    desc = (
        f"**{symbol}** — **{side}**\n"
        f"**Leverage:** {lev}x\n"
        f"**Entry:** [{eh:.6f} → {el:.6f}]\n"
        f"**Stop:** {sl:.6f}\n"
        f"**TPs:** {', '.join(f'{tp:.6f}' for tp in tps)}"
    )
    if extras:
        desc += "\n" + "\n".join(f"- {k}: {v}" for k, v in extras.items())
    payload = {"embeds": [{"title": f"{symbol} Signal", "description": desc}]}
    r = requests.post(WEBHOOK, json=payload, timeout=10)
    r.raise_for_status()

def send_info(msg: str):
    payload = {"embeds": [{"title": "Signals Bot", "description": msg}]}
    r = requests.post(WEBHOOK, json=payload, timeout=10)
    r.raise_for_status()

