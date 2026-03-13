#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Uniswap V3 LP Indexer (Base)
- First run: full backfill from FROM_BLOCK
- Subsequent runs: only new blocks since last sync
- State persisted per-pool in state/<pool>/sync.json + events.ndjson
- Exports per-pool CSV files into out/<pool>/
- Price buckets are in USD per 1 LMTS
- TokenId tracked via NPM IncreaseLiquidity events (lightweight)
- Current owner resolved via NPM.ownerOf(tokenId) eth_call at end of run
- Minter resolved via NPM.positions() eth_call (operator field not available,
  so minter = current_owner on first mint; tracked in state/token_ids.json)
"""

import os
import json
import csv
import time
from decimal import Decimal, getcontext
from collections import defaultdict

from web3 import Web3
from eth_abi import decode as abi_decode

getcontext().prec = 80

# =========================================================
# CONFIG
# =========================================================
RPC_URL = os.environ["BASE_RPC_URL"]

CHAIN_ID = 8453
POOL = Web3.to_checksum_address("0x132531f014002aCC9a9CCc98057Fe05a88070AcE")
FROM_BLOCK = 43310374

# Uniswap V3 NonfungiblePositionManager on Base
NPM = Web3.to_checksum_address("0x03a520b32C04BF3bEEf7BEb72E919cf822Ed34f1")

OUR_WALLETS = {
    "0x5f0aea872b7d6dbcc181338f80048b130e443e3b": "our_pool_wallet",
    "0x44a3f0354f4c10eb9cd93e522b5e3210d126f054": "team_mm2",
}

POOL_ID = POOL.lower()
STATE_DIR = os.path.join("state", POOL_ID)
SYNC_FILE = os.path.join(STATE_DIR, "sync.json")
EVENTS_FILE = os.path.join(STATE_DIR, "events.ndjson")
TOKEN_IDS_FILE = os.path.join(STATE_DIR, "token_ids.json")
OUT_DIR = os.path.join("out", POOL_ID)

CHUNK_SIZE = 2000
SLEEP_BETWEEN_CHUNKS = 0.15
RETRIES = 5
RETRY_SLEEP = 2.0
TIMEOUT = 30
CONFIRMATIONS_BUFFER = 5

# price bucket step in USD per 1 LMTS
PRICE_BUCKET_SIZE = Decimal("0.01")

EXPECTED_TOKEN0 = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913".lower()  # USDC
EXPECTED_TOKEN1 = "0x9EadbE35F3Ee3bF3e28180070C429298a1b02F93".lower()  # LMTS

# =========================================================
# WEB3
# =========================================================
w3 = Web3(Web3.HTTPProvider(RPC_URL, request_kwargs={"timeout": TIMEOUT}))
if not w3.is_connected():
    raise RuntimeError("RPC not connected — check BASE_RPC_URL secret")

# =========================================================
# ABIs
# =========================================================
POOL_ABI = [
    {"name": "token0", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "address"}]},
    {"name": "token1", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "address"}]},
    {"name": "tickSpacing", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "int24"}]},
    {"name": "fee", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "uint24"}]},
    {"name": "slot0", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [
        {"type": "uint160"},
        {"type": "int24"},
        {"type": "uint16"},
        {"type": "uint16"},
        {"type": "uint16"},
        {"type": "uint8"},
        {"type": "bool"},
    ]},
    {"name": "liquidity", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "uint128"}]},
]

ERC20_ABI = [
    {"name": "decimals", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "uint8"}]},
    {"name": "symbol", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [{"type": "string"}]},
]

NPM_ABI = [
    {
        "name": "ownerOf",
        "type": "function",
        "stateMutability": "view",
        "inputs": [{"name": "tokenId", "type": "uint256"}],
        "outputs": [{"type": "address"}],
    },
]

pool_c = w3.eth.contract(address=POOL, abi=POOL_ABI)
npm_c = w3.eth.contract(address=NPM, abi=NPM_ABI)

# =========================================================
# TOPICS
# =========================================================
MINT_TOPIC = w3.keccak(text="Mint(address,address,int24,int24,uint128,uint256,uint256)").hex()
BURN_TOPIC = w3.keccak(text="Burn(address,int24,int24,uint128,uint256,uint256)").hex()

# NPM: IncreaseLiquidity(uint256 indexed tokenId, uint128 liquidity, uint256 amount0, uint256 amount1)
INCREASE_LIQUIDITY_TOPIC = w3.keccak(text="IncreaseLiquidity(uint256,uint128,uint256,uint256)").hex()

# =========================================================
# MATH
# =========================================================
Q96 = Decimal(2) ** 96
D_1_0001 = Decimal("1.0001")


def sqrt_price_x96_to_price_token1_per_token0(sqrt_price_x96: int, dec0: int, dec1: int) -> Decimal:
    ratio = (Decimal(sqrt_price_x96) / Q96) ** 2
    return ratio * (Decimal(10) ** Decimal(dec0 - dec1))


def tick_to_sqrt_price(tick: int) -> Decimal:
    return D_1_0001 ** (Decimal(tick) / Decimal(2))


def current_amounts_from_liquidity(liq: int, tick_lower: int, tick_upper: int, sqrt_price_x96: int):
    L = Decimal(liq)
    sqrtP = Decimal(sqrt_price_x96) / Q96
    sqrtL = tick_to_sqrt_price(tick_lower)
    sqrtU = tick_to_sqrt_price(tick_upper)

    if sqrtP <= sqrtL:
        amount0 = L * (sqrtU - sqrtL) / (sqrtL * sqrtU)
        amount1 = Decimal(0)
    elif sqrtP < sqrtU:
        amount0 = L * (sqrtU - sqrtP) / (sqrtP * sqrtU)
        amount1 = L * (sqrtP - sqrtL)
    else:
        amount0 = Decimal(0)
        amount1 = L * (sqrtU - sqrtL)

    return amount0, amount1


def human(raw, decimals: int) -> Decimal:
    return Decimal(raw) / (Decimal(10) ** Decimal(decimals))


def fmt(x, places: int = 6) -> str:
    x = Decimal(x)
    q = Decimal(10) ** -places
    return str(x.quantize(q))


def tick_to_price_token1_per_token0(tick: int, dec0: int, dec1: int) -> Decimal:
    return (Decimal("1.0001") ** Decimal(tick)) * (Decimal(10) ** Decimal(dec0 - dec1))


def tick_to_price_usdc_per_lmts(tick: int, dec0: int, dec1: int) -> Decimal:
    lmts_per_usdc = tick_to_price_token1_per_token0(tick, dec0, dec1)
    if lmts_per_usdc == 0:
        return Decimal(0)
    return Decimal(1) / lmts_per_usdc


def position_price_range_usdc_per_lmts(tick_lower: int, tick_upper: int, dec0: int, dec1: int) -> tuple:
    p1 = tick_to_price_usdc_per_lmts(tick_lower, dec0, dec1)
    p2 = tick_to_price_usdc_per_lmts(tick_upper, dec0, dec1)
    return min(p1, p2), max(p1, p2)


def price_bucket_floor(price: Decimal, step: Decimal) -> Decimal:
    return (price // step) * step


# =========================================================
# RPC HELPERS
# =========================================================
def safe_call(fn):
    last = None
    for attempt in range(RETRIES):
        try:
            return fn.call()
        except Exception as e:
            last = e
            wait = RETRY_SLEEP * (attempt + 1)
            print(f"  [warn] call failed ({e}), retry in {wait:.1f}s")
            time.sleep(wait)
    raise last


def get_logs_with_retry(params: dict) -> list:
    fixed = dict(params)
    if "fromBlock" in fixed and isinstance(fixed["fromBlock"], int):
        fixed["fromBlock"] = hex(fixed["fromBlock"])
    if "toBlock" in fixed and isinstance(fixed["toBlock"], int):
        fixed["toBlock"] = hex(fixed["toBlock"])

    last = None
    for attempt in range(RETRIES):
        try:
            return w3.eth.get_logs(fixed)
        except Exception as e:
            last = e
            wait = RETRY_SLEEP * (attempt + 1)
            msg = str(e)
            if "query returned more than" in msg or "block range" in msg.lower() or "limit exceeded" in msg.lower():
                raise
            print(f"  [warn] get_logs failed ({e}), retry in {wait:.1f}s")
            time.sleep(wait)
    raise last


def get_logs_safe(from_block: int, to_block: int, topic: str, address: str = None) -> list:
    if from_block > to_block:
        return []
    addr = address or str(POOL)
    try:
        return get_logs_with_retry({
            "fromBlock": from_block,
            "toBlock": to_block,
            "address": addr,
            "topics": [topic],
        })
    except Exception as e:
        if from_block == to_block:
            print(f"  [error] single block {from_block} failed: {e}")
            return []
        mid = (from_block + to_block) // 2
        left = get_logs_safe(from_block, mid, topic, address)
        right = get_logs_safe(mid + 1, to_block, topic, address)
        return left + right


# =========================================================
# DECODE
# =========================================================
def _to_bytes(data) -> bytes:
    if isinstance(data, (bytes, bytearray)):
        return bytes(data)
    if hasattr(data, "hex"):
        return bytes(data)
    s = str(data)
    if s.startswith("0x") or s.startswith("0X"):
        s = s[2:]
    return bytes.fromhex(s)


def topic_to_address(topic) -> str:
    raw = _to_bytes(topic).hex()
    return Web3.to_checksum_address("0x" + raw[-40:]).lower()


def topic_to_uint256(topic) -> int:
    return int(_to_bytes(topic).hex(), 16)


def decode_int24_topic(topic) -> int:
    raw = _to_bytes(topic).hex().zfill(64)
    val = int(raw[-6:], 16)
    if val >= 2 ** 23:
        val -= 2 ** 24
    return val


def decode_mint_data(data) -> tuple:
    b = _to_bytes(data)
    sender, liquidity, amount0, amount1 = abi_decode(
        ["address", "uint128", "uint256", "uint256"], b
    )
    return str(sender).lower(), int(liquidity), int(amount0), int(amount1)


def decode_burn_data(data) -> tuple:
    b = _to_bytes(data)
    liquidity, amount0, amount1 = abi_decode(["uint128", "uint256", "uint256"], b)
    return int(liquidity), int(amount0), int(amount1)


def position_key(owner: str, tick_lower: int, tick_upper: int) -> str:
    return f"{owner.lower()}|{tick_lower}|{tick_upper}"


# =========================================================
# STATE
# =========================================================
def ensure_dirs():
    os.makedirs(STATE_DIR, exist_ok=True)
    os.makedirs(OUT_DIR, exist_ok=True)


def load_sync() -> dict:
    if not os.path.exists(SYNC_FILE):
        return {"chain_id": CHAIN_ID, "pool": str(POOL), "last_scanned_block": None}
    with open(SYNC_FILE, encoding="utf-8") as f:
        return json.load(f)


def save_sync(sync: dict):
    with open(SYNC_FILE, "w", encoding="utf-8") as f:
        json.dump(sync, f, ensure_ascii=False, indent=2, sort_keys=True)


def load_token_ids() -> dict:
    """
    Returns dict: str(token_id) -> {minter, current_owner}
    Persisted across runs so minter is never lost.
    """
    if not os.path.exists(TOKEN_IDS_FILE):
        return {}
    with open(TOKEN_IDS_FILE, encoding="utf-8") as f:
        return json.load(f)


def save_token_ids(data: dict):
    with open(TOKEN_IDS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, sort_keys=True)


def load_event_ids() -> set:
    ids = set()
    if not os.path.exists(EVENTS_FILE):
        return ids
    with open(EVENTS_FILE, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                ids.add(json.loads(line)["event_id"])
    return ids


def append_events(events: list):
    if not events:
        return
    with open(EVENTS_FILE, "a", encoding="utf-8") as f:
        for ev in events:
            f.write(json.dumps(ev, ensure_ascii=False, sort_keys=True) + "\n")


# =========================================================
# COLLECT NEW EVENTS
# =========================================================
def collect_events(sync: dict, end_block: int) -> dict:
    last = sync.get("last_scanned_block")
    start = FROM_BLOCK if last is None else int(last) + 1

    if start > end_block:
        print(f"Nothing to scan (start={start} > end={end_block})")
        return {"scanned_blocks": 0, "mint": 0, "burn": 0, "written": 0}

    total_blocks = end_block - start + 1
    chunks = list(range(start, end_block + 1, CHUNK_SIZE))
    total_chunks = len(chunks)

    print(f"Scanning blocks {start} -> {end_block} ({total_blocks:,} blocks, {total_chunks} chunks of {CHUNK_SIZE})")

    existing_ids = load_event_ids()
    total_mint = total_burn = total_written = 0

    for i, chunk_start in enumerate(chunks, 1):
        chunk_end = min(chunk_start + CHUNK_SIZE - 1, end_block)

        # Pool events
        mint_logs = get_logs_safe(chunk_start, chunk_end, MINT_TOPIC)
        burn_logs = get_logs_safe(chunk_start, chunk_end, BURN_TOPIC)

        # NPM IncreaseLiquidity — tx_hash -> tokenId (only our pool's mints)
        npm_il_logs = get_logs_safe(chunk_start, chunk_end, INCREASE_LIQUIDITY_TOPIC, str(NPM))
        npm_token_ids: dict = {}
        for lg in npm_il_logs:
            tx = lg["transactionHash"].hex().lower()
            if len(lg["topics"]) >= 2:
                npm_token_ids[tx] = topic_to_uint256(lg["topics"][1])

        new_events = []

        # --- MINT ---
        for lg in mint_logs:
            tx = lg["transactionHash"].hex().lower()
            idx = int(lg["logIndex"])
            eid = f"mint:{tx}:{idx}"
            if eid in existing_ids:
                continue
            if len(lg["topics"]) < 4:
                continue

            owner = topic_to_address(lg["topics"][1])
            tl = decode_int24_topic(lg["topics"][2])
            tu = decode_int24_topic(lg["topics"][3])
            _, liq, a0, a1 = decode_mint_data(lg["data"])
            token_id = npm_token_ids.get(tx)

            new_events.append({
                "event_id": eid,
                "event_type": "mint",
                "block_number": int(lg["blockNumber"]),
                "tx_hash": tx,
                "log_index": idx,
                "owner": owner,
                "tick_lower": tl,
                "tick_upper": tu,
                "liquidity_delta_raw": liq,
                "amount0_raw": a0,
                "amount1_raw": a1,
                "token_id": token_id,
            })
            existing_ids.add(eid)
            total_mint += 1

        # --- BURN ---
        for lg in burn_logs:
            tx = lg["transactionHash"].hex().lower()
            idx = int(lg["logIndex"])
            eid = f"burn:{tx}:{idx}"
            if eid in existing_ids:
                continue
            if len(lg["topics"]) < 4:
                continue

            owner = topic_to_address(lg["topics"][1])
            tl = decode_int24_topic(lg["topics"][2])
            tu = decode_int24_topic(lg["topics"][3])
            liq, a0, a1 = decode_burn_data(lg["data"])

            new_events.append({
                "event_id": eid,
                "event_type": "burn",
                "block_number": int(lg["blockNumber"]),
                "tx_hash": tx,
                "log_index": idx,
                "owner": owner,
                "tick_lower": tl,
                "tick_upper": tu,
                "liquidity_delta_raw": -liq,
                "amount0_raw": a0,
                "amount1_raw": a1,
                "token_id": None,
            })
            existing_ids.add(eid)
            total_burn += 1

        append_events(new_events)
        total_written += len(new_events)

        sync["last_scanned_block"] = chunk_end
        save_sync(sync)

        if i % 20 == 0 or i == total_chunks:
            pct = i / total_chunks * 100
            print(
                f"  chunk {i}/{total_chunks} ({pct:.0f}%) block {chunk_end} | "
                f"mint={total_mint} burn={total_burn} written={total_written}"
            )

        time.sleep(SLEEP_BETWEEN_CHUNKS)

    return {
        "scanned_blocks": total_blocks,
        "mint": total_mint,
        "burn": total_burn,
        "written": total_written,
    }


# =========================================================
# RESOLVE TOKEN IDS via ownerOf
# =========================================================
def resolve_token_ids(positions: dict, token_ids_state: dict) -> dict:
    """
    For every active position with a token_id, call NPM.ownerOf(tokenId).
    Preserves minter from previous runs (never overwritten).
    Updates current_owner on every run.
    Returns updated token_ids_state.
    """
    # Collect all tokenIds from active positions
    active_token_ids = set()
    for row in positions.values():
        if row["liquidity_raw"] > 0 and row.get("token_id") is not None:
            active_token_ids.add(row["token_id"])

    if not active_token_ids:
        return token_ids_state

    print(f"  Resolving {len(active_token_ids)} tokenId(s) via ownerOf()...")

    for token_id in sorted(active_token_ids):
        key = str(token_id)
        if key not in token_ids_state:
            token_ids_state[key] = {"minter": None, "current_owner": None}

        try:
            owner = npm_c.functions.ownerOf(token_id).call().lower()
        except Exception as e:
            # Token burned / closed — ownerOf reverts
            owner = None
            print(f"  [warn] ownerOf({token_id}) failed ({e}) — position may be closed")

        # Set minter only once (on first successful resolve)
        if token_ids_state[key]["minter"] is None and owner is not None:
            token_ids_state[key]["minter"] = owner

        token_ids_state[key]["current_owner"] = owner
        time.sleep(0.05)  # gentle rate limit between calls

    return token_ids_state


# =========================================================
# REBUILD POSITIONS
# =========================================================
def rebuild_positions() -> dict:
    positions = {}
    if not os.path.exists(EVENTS_FILE):
        return positions

    with open(EVENTS_FILE, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            ev = json.loads(line)
            if ev["event_type"] not in ("mint", "burn"):
                continue

            owner = ev["owner"]
            tl = int(ev["tick_lower"])
            tu = int(ev["tick_upper"])
            key = position_key(owner, tl, tu)

            if key not in positions:
                positions[key] = {
                    "owner": owner,
                    "tick_lower": tl,
                    "tick_upper": tu,
                    "liquidity_raw": 0,
                    "minted_amount0_raw": 0,
                    "minted_amount1_raw": 0,
                    "burned_amount0_raw": 0,
                    "burned_amount1_raw": 0,
                    "first_seen_block": ev["block_number"],
                    "last_seen_block": ev["block_number"],
                    "mint_count": 0,
                    "burn_count": 0,
                    "token_id": None,
                }

            row = positions[key]
            row["first_seen_block"] = min(row["first_seen_block"], ev["block_number"])
            row["last_seen_block"] = max(row["last_seen_block"], ev["block_number"])

            if ev.get("token_id") is not None:
                row["token_id"] = ev["token_id"]

            liq_delta = int(ev["liquidity_delta_raw"])
            a0 = int(ev["amount0_raw"])
            a1 = int(ev["amount1_raw"])
            row["liquidity_raw"] += liq_delta

            if ev["event_type"] == "mint":
                row["minted_amount0_raw"] += a0
                row["minted_amount1_raw"] += a1
                row["mint_count"] += 1
            else:
                row["burned_amount0_raw"] += a0
                row["burned_amount1_raw"] += a1
                row["burn_count"] += 1

    return positions


# =========================================================
# POOL INFO
# =========================================================
def load_pool_info() -> dict:
    token0 = Web3.to_checksum_address(safe_call(pool_c.functions.token0()))
    token1 = Web3.to_checksum_address(safe_call(pool_c.functions.token1()))
    tick_spacing = int(safe_call(pool_c.functions.tickSpacing()))
    fee = int(safe_call(pool_c.functions.fee()))
    slot0 = safe_call(pool_c.functions.slot0())
    pool_liq = int(safe_call(pool_c.functions.liquidity()))

    sqrt_price_x96 = int(slot0[0])
    current_tick = int(slot0[1])

    t0 = w3.eth.contract(address=token0, abi=ERC20_ABI)
    t1 = w3.eth.contract(address=token1, abi=ERC20_ABI)
    dec0 = int(safe_call(t0.functions.decimals()))
    dec1 = int(safe_call(t1.functions.decimals()))
    sym0 = safe_call(t0.functions.symbol())
    sym1 = safe_call(t1.functions.symbol())

    return {
        "token0": token0,
        "token1": token1,
        "dec0": dec0,
        "dec1": dec1,
        "sym0": sym0,
        "sym1": sym1,
        "tick_spacing": tick_spacing,
        "fee": fee,
        "sqrt_price_x96": sqrt_price_x96,
        "current_tick": current_tick,
        "price_token1_per_token0": sqrt_price_x96_to_price_token1_per_token0(sqrt_price_x96, dec0, dec1),
        "pool_liquidity_raw": pool_liq,
    }


# =========================================================
# BUILD CSV EXPORTS
# =========================================================
def build_exports(positions: dict, token_ids_state: dict, pool_info: dict):
    sym0 = pool_info["sym0"].lower()
    sym1 = pool_info["sym1"].lower()
    dec0 = pool_info["dec0"]
    dec1 = pool_info["dec1"]
    current_tick = pool_info["current_tick"]
    sqrt_price_x96 = pool_info["sqrt_price_x96"]

    positions_rows = []
    owner_aggr = defaultdict(lambda: {
        "label": "external",
        "type": "external",
        "liquidity_raw": 0,
        "positions": 0,
        "in_range": 0,
        "cur0": Decimal(0),
        "cur1": Decimal(0),
        "token_ids": [],
    })

    bucket_map = defaultdict(lambda: {
        "liq": 0, "our_liq": 0, "ext_liq": 0,
        "pos": 0, "our_pos": 0, "ext_pos": 0,
        "owners": set(),
    })

    our_liq = 0
    ext_liq = 0
    our_cur0 = Decimal(0)
    our_cur1 = Decimal(0)
    ext_cur0 = Decimal(0)
    ext_cur1 = Decimal(0)

    for row in positions.values():
        liq = int(row["liquidity_raw"])
        if liq <= 0:
            continue

        pool_owner = row["owner"].lower()
        tl = int(row["tick_lower"])
        tu = int(row["tick_upper"])
        in_range = tl <= current_tick < tu
        token_id = row.get("token_id")

        # Resolve real owner from token_ids_state
        minter = ""
        current_owner = ""
        real_owner = pool_owner  # fallback (e.g. direct mint bypassing NPM)

        if token_id is not None:
            nft = token_ids_state.get(str(token_id), {})
            minter = nft.get("minter") or ""
            current_owner = nft.get("current_owner") or ""
            if current_owner:
                real_owner = current_owner
            elif minter:
                real_owner = minter

        label = OUR_WALLETS.get(real_owner, "external")
        otype = "ours" if real_owner in OUR_WALLETS else "external"

        price_lower, price_upper = position_price_range_usdc_per_lmts(tl, tu, dec0, dec1)

        cur0_dec, cur1_dec = current_amounts_from_liquidity(liq, tl, tu, sqrt_price_x96)
        cur0 = human(cur0_dec, dec0)
        cur1 = human(cur1_dec, dec1)

        net0 = human(row["minted_amount0_raw"] - row["burned_amount0_raw"], dec0)
        net1 = human(row["minted_amount1_raw"] - row["burned_amount1_raw"], dec1)

        if otype == "ours":
            our_liq += liq
            our_cur0 += cur0
            our_cur1 += cur1
        else:
            ext_liq += liq
            ext_cur0 += cur0
            ext_cur1 += cur1

        positions_rows.append({
            "token_id": token_id if token_id is not None else "",
            "minter": minter,
            "current_owner": current_owner,
            "label": label,
            "type": otype,
            "pool_owner": pool_owner,
            "tick_lower": tl,
            "tick_upper": tu,
            "width_ticks": tu - tl,
            "price_lower_usd_per_lmts": fmt(price_lower, 6),
            "price_upper_usd_per_lmts": fmt(price_upper, 6),
            "liquidity": liq,
            "in_range": in_range,
            f"cur_{sym0}": fmt(cur0),
            f"cur_{sym1}": fmt(cur1),
            f"net_{sym0}": fmt(net0),
            f"net_{sym1}": fmt(net1),
            f"minted_{sym0}": fmt(human(row["minted_amount0_raw"], dec0)),
            f"minted_{sym1}": fmt(human(row["minted_amount1_raw"], dec1)),
            f"burned_{sym0}": fmt(human(row["burned_amount0_raw"], dec0)),
            f"burned_{sym1}": fmt(human(row["burned_amount1_raw"], dec1)),
            "mint_count": row["mint_count"],
            "burn_count": row["burn_count"],
            "first_block": row["first_seen_block"],
            "last_block": row["last_seen_block"],
        })

        ag = owner_aggr[real_owner]
        ag["label"] = label
        ag["type"] = otype
        ag["liquidity_raw"] += liq
        ag["positions"] += 1
        ag["in_range"] += 1 if in_range else 0
        ag["cur0"] += cur0
        ag["cur1"] += cur1
        if token_id is not None:
            ag["token_ids"].append(str(token_id))

        b_start = price_bucket_floor(price_lower, PRICE_BUCKET_SIZE)
        b_end = price_bucket_floor(price_upper, PRICE_BUCKET_SIZE)
        b = b_start
        while b <= b_end:
            bm = bucket_map[str(b)]
            bm["liq"] += liq
            bm["pos"] += 1
            bm["owners"].add(real_owner)
            if otype == "ours":
                bm["our_liq"] += liq
                bm["our_pos"] += 1
            else:
                bm["ext_liq"] += liq
                bm["ext_pos"] += 1
            b += PRICE_BUCKET_SIZE

    positions_rows.sort(key=lambda r: (r["type"] != "ours", -r["liquidity"], r["current_owner"]))

    top_lp_rows = []
    for owner, ag in owner_aggr.items():
        top_lp_rows.append({
            "owner": owner,
            "label": ag["label"],
            "type": ag["type"],
            "liquidity": ag["liquidity_raw"],
            "positions": ag["positions"],
            "in_range_positions": ag["in_range"],
            "token_ids": ",".join(ag["token_ids"]),
            f"cur_{sym0}": fmt(ag["cur0"]),
            f"cur_{sym1}": fmt(ag["cur1"]),
        })
    top_lp_rows.sort(key=lambda r: (r["type"] != "ours", -r["liquidity"]))

    current_price_usd_per_lmts = Decimal(1) / pool_info["price_token1_per_token0"]

    bucket_rows = []
    for bk_str in sorted(bucket_map.keys(), key=lambda x: Decimal(x)):
        bm = bucket_map[bk_str]
        bk = Decimal(bk_str)
        upper = bk + PRICE_BUCKET_SIZE
        bucket_rows.append({
            "price_lower_usd_per_lmts": fmt(bk, 6),
            "price_upper_usd_per_lmts": fmt(upper, 6),
            "liquidity": bm["liq"],
            "our_liquidity": bm["our_liq"],
            "ext_liquidity": bm["ext_liq"],
            "positions": bm["pos"],
            "our_positions": bm["our_pos"],
            "ext_positions": bm["ext_pos"],
            "unique_owners": len(bm["owners"]),
            "contains_current_price": bk <= current_price_usd_per_lmts < upper,
        })

    open_pos = len(positions_rows)
    in_range_pos = sum(1 for r in positions_rows if r["in_range"])

    summary_rows = [
        {"metric": "pool", "value": str(POOL)},
        {"metric": "npm", "value": str(NPM)},
        {"metric": "chain_id", "value": CHAIN_ID},
        {"metric": "from_block", "value": FROM_BLOCK},
        {"metric": f"token0_{sym0}", "value": pool_info["token0"]},
        {"metric": f"token1_{sym1}", "value": pool_info["token1"]},
        {"metric": "dec0", "value": dec0},
        {"metric": "dec1", "value": dec1},
        {"metric": "fee", "value": pool_info["fee"]},
        {"metric": "tick_spacing", "value": pool_info["tick_spacing"]},
        {"metric": "current_tick", "value": current_tick},
        {"metric": f"price_{sym1}_per_{sym0}", "value": fmt(pool_info["price_token1_per_token0"], 8)},
        {"metric": "price_usd_per_lmts", "value": fmt(current_price_usd_per_lmts, 8)},
        {"metric": "pool_liquidity_raw", "value": pool_info["pool_liquidity_raw"]},
        {"metric": "open_positions", "value": open_pos},
        {"metric": "in_range_positions", "value": in_range_pos},
        {"metric": "unique_real_owners", "value": len(owner_aggr)},
        {"metric": "our_positions", "value": sum(1 for r in positions_rows if r["type"] == "ours")},
        {"metric": "our_liquidity", "value": our_liq},
        {"metric": "ext_liquidity", "value": ext_liq},
        {"metric": f"our_cur_{sym0}", "value": fmt(our_cur0)},
        {"metric": f"our_cur_{sym1}", "value": fmt(our_cur1)},
        {"metric": f"ext_cur_{sym0}", "value": fmt(ext_cur0)},
        {"metric": f"ext_cur_{sym1}", "value": fmt(ext_cur1)},
    ]

    return positions_rows, top_lp_rows, bucket_rows, summary_rows


# =========================================================
# CSV WRITER
# =========================================================
def write_csv(path: str, rows: list, fieldnames: list = None):
    fieldnames = fieldnames or (list(rows[0].keys()) if rows else [])
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        if rows:
            w.writerows(rows)
    print(f"  -> {path} ({len(rows)} rows)")


# =========================================================
# MAIN
# =========================================================
def main():
    ensure_dirs()

    latest = int(w3.eth.block_number)
    end_block = latest - CONFIRMATIONS_BUFFER
    if end_block < FROM_BLOCK:
        raise RuntimeError(f"end_block ({end_block}) < FROM_BLOCK ({FROM_BLOCK})")

    sync = load_sync()
    pool_info = load_pool_info()

    print("=" * 60)
    print("POOL INFO")
    print("=" * 60)
    print(f"Pool:           {POOL}")
    print(f"NPM:            {NPM}")
    print(f"Tokens:         {pool_info['sym0']} / {pool_info['sym1']}")
    print(f"Fee:            {pool_info['fee']}  tick_spacing: {pool_info['tick_spacing']}")
    print(f"Current tick:   {pool_info['current_tick']}")
    print(f"Price {pool_info['sym1']}/{pool_info['sym0']}: {fmt(pool_info['price_token1_per_token0'], 8)}")
    print(f"Price USD/LMTS: {fmt(Decimal(1) / pool_info['price_token1_per_token0'], 8)}")
    print(f"Pool liq raw:   {pool_info['pool_liquidity_raw']:,}")
    print(f"Latest block:   {latest}  safe: {end_block}")
    last = sync.get("last_scanned_block")
    print(f"Last synced:    {last if last else 'never (first run)'}")
    print()

    if pool_info["token0"].lower() != EXPECTED_TOKEN0:
        print(f"WARNING: token0 mismatch! got {pool_info['token0']}")
    if pool_info["token1"].lower() != EXPECTED_TOKEN1:
        print(f"WARNING: token1 mismatch! got {pool_info['token1']}")

    print("=" * 60)
    print("COLLECTING EVENTS")
    print("=" * 60)
    stats = collect_events(sync, end_block)
    for k, v in stats.items():
        print(f"  {k}: {v:,}" if isinstance(v, int) else f"  {k}: {v}")

    print()
    print("=" * 60)
    print("REBUILDING SNAPSHOT")
    print("=" * 60)
    positions = rebuild_positions()
    active = sum(1 for r in positions.values() if r["liquidity_raw"] > 0)
    print(f"  total position keys: {len(positions):,}")
    print(f"  active (liq > 0):    {active:,}")

    print()
    print("=" * 60)
    print("RESOLVING TOKEN OWNERS")
    print("=" * 60)
    token_ids_state = load_token_ids()
    token_ids_state = resolve_token_ids(positions, token_ids_state)
    save_token_ids(token_ids_state)
    print(f"  total tokenIds tracked: {len(token_ids_state)}")

    print()
    print("=" * 60)
    print("WRITING EXPORTS")
    print("=" * 60)
    pos_rows, lp_rows, bucket_rows, summary_rows = build_exports(positions, token_ids_state, pool_info)

    write_csv(os.path.join(OUT_DIR, "open_positions.csv"), pos_rows)
    write_csv(os.path.join(OUT_DIR, "top_lp.csv"), lp_rows)
    write_csv(os.path.join(OUT_DIR, "buckets.csv"), bucket_rows)
    write_csv(os.path.join(OUT_DIR, "summary.csv"), summary_rows, fieldnames=["metric", "value"])

    print()
    print("=== SUMMARY ===")
    for row in summary_rows:
        print(f"  {row['metric']}: {row['value']}")

    print("\nDONE")


if __name__ == "__main__":
    main()
