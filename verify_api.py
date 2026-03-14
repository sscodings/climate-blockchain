
import os, hashlib, requests, logging
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from web3 import Web3
from dotenv import load_dotenv

load_dotenv()

RPC_URL          = os.environ.get("RPC_URL")
CONTRACT_ADDRESS = os.environ.get("CONTRACT_ADDRESS")
PRIVATE_KEY      = os.environ.get("PRIVATE_KEY")

CONTRACT_ABI = [
    {
        "inputs": [
            {"name": "_fileName",    "type": "string"},
            {"name": "_fileHash",    "type": "bytes32"},
            {"name": "_tempColHash", "type": "bytes32"},
            {"name": "_latColHash",  "type": "bytes32"},
            {"name": "_lonColHash",  "type": "bytes32"},
            {"name": "_totalRows",   "type": "uint256"},
        ],
        "name": "registerDataset",
        "outputs": [{"name": "id", "type": "uint256"}],
        "stateMutability": "nonpayable",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "recordCount",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"name": "_id", "type": "uint256"}],
        "name": "getRecord",
        "outputs": [
            {"name": "fileName",     "type": "string"},
            {"name": "fileHash",     "type": "bytes32"},
            {"name": "tempColHash",  "type": "bytes32"},
            {"name": "latColHash",   "type": "bytes32"},
            {"name": "lonColHash",   "type": "bytes32"},
            {"name": "totalRows",    "type": "uint256"},
            {"name": "registeredAt", "type": "uint256"},
            {"name": "registeredBy", "type": "address"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
]


logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
log = logging.getLogger("verifier")


app = FastAPI(
    title="Climate Data Blockchain Verifier",
    description="Hash climate CSVs and verify integrity on Sepolia blockchain",
    version="1.0.0"
)


class RegisterRequest(BaseModel):
    csv_url: str

class VerifyRequest(BaseModel):
    csv_url:   str
    record_id: int


def _fetch_and_hash(csv_url: str) -> dict:
    """Download CSV from URL and compute SHA-256 hashes."""
    resp = requests.get(csv_url, timeout=30)
    resp.raise_for_status()

    text  = resp.content.decode("utf-8")
    sep   = "\r\n" if "\r\n" in text else "\n"
    lines = text.strip().split(sep)
    header, rows = lines[0], lines[1:]


    normalized = (header + "\n" + "\n".join(sorted(rows))).encode("utf-8")
    file_hash  = hashlib.sha256(normalized).hexdigest()

    temps, lats, lons = [], [], []
    for r in rows:
        p = r.split(",")
        if len(p) >= 4:
            temps.append(p[1]); lats.append(p[2]); lons.append(p[3])

    return {
        "file_name":  csv_url.split("/")[-1].split("?")[0],
        "total_rows": len(rows),
        "file_hash":  "0x" + file_hash,
        "col_hashes": {
            "temperature": "0x" + hashlib.sha256("\n".join(temps).encode()).hexdigest(),
            "latitude":    "0x" + hashlib.sha256("\n".join(lats).encode()).hexdigest(),
            "longitude":   "0x" + hashlib.sha256("\n".join(lons).encode()).hexdigest(),
        },
    }

def _connect():
    """Connect to Sepolia and return w3, contract, account."""
    w3       = Web3(Web3.HTTPProvider(RPC_URL))
    contract = w3.eth.contract(address=CONTRACT_ADDRESS, abi=CONTRACT_ABI)
    account  = w3.eth.account.from_key(PRIVATE_KEY)
    return w3, contract, account

def _to_bytes32(hex_str: str) -> bytes:
    return bytes.fromhex(hex_str.replace("0x", "").ljust(64, "0"))

def _send_tx(w3, account, fn):
    """Build, sign and send a transaction. Returns receipt."""
    tx = fn.build_transaction({
        "from":     account.address,
        "nonce":    w3.eth.get_transaction_count(account.address),
        "gas":      300000,
        "gasPrice": w3.eth.gas_price,
    })
    signed  = account.sign_transaction(tx, PRIVATE_KEY)
    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
    return w3.eth.wait_for_transaction_receipt(tx_hash)


@app.get("/")
def root():
    return {
        "project":   "Climate Data Blockchain Verifier",
        "endpoints": {
            "POST /register":    "First time — hash CSV and store on blockchain",
            "POST /verify":      "Every time — compare CSV hash against stored",
            "GET  /record/{id}": "Read any stored record from chain"
        }
    }


@app.post("/register")
def register(req: RegisterRequest):
    """
    Call once when your partner shares the CSV URL for the first time.
    Hashes the file and stores all hashes on the blockchain.
    Returns record_id — save this number, you will need it for /verify.
    """
    log.info(f"[REGISTER] {req.csv_url}")
    try:
        hashes          = _fetch_and_hash(req.csv_url)
        w3, contract, account = _connect()

        receipt   = _send_tx(w3, account, contract.functions.registerDataset(
            hashes["file_name"],
            _to_bytes32(hashes["file_hash"]),
            _to_bytes32(hashes["col_hashes"]["temperature"]),
            _to_bytes32(hashes["col_hashes"]["latitude"]),
            _to_bytes32(hashes["col_hashes"]["longitude"]),
            hashes["total_rows"],
        ))
        record_id = contract.functions.recordCount().call()
        log.info(f"[REGISTER] block #{receipt.blockNumber} record_id={record_id}")

        return {
            "status":     "registered",
            "record_id":  record_id,
            "file_name":  hashes["file_name"],
            "total_rows": hashes["total_rows"],
            "file_hash":  hashes["file_hash"],
            "tx_hash":    receipt.transactionHash.hex(),
            "block":      receipt.blockNumber,
            "message":    "Hash stored on blockchain. Save record_id for /verify."
        }
    except Exception as e:
        log.error(f"[REGISTER] {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/verify")
def verify(req: VerifyRequest):
    """
    Call every time your partner sends a new CSV URL.
    Re-hashes the file and compares against the stored on-chain hash.

    Your backend reads tampered: true/false and acts accordingly.

    Intact  → { "tampered": false, "status": "intact",   ... }
    Tampered → { "tampered": true,  "status": "tampered", "columns": {...}, ... }
    """
    log.info(f"[VERIFY] record_id={req.record_id}")
    try:
        current       = _fetch_and_hash(req.csv_url)
        _, contract, _ = _connect()

        stored = contract.functions.getRecord(req.record_id).call()
        stored_file_hash  = "0x" + stored[1].hex()
        stored_col_hashes = {
            "temperature": "0x" + stored[2].hex(),
            "latitude":    "0x" + stored[3].hex(),
            "longitude":   "0x" + stored[4].hex(),
        }

        intact = current["file_hash"] == stored_file_hash

        if intact:
            log.info("[VERIFY] INTACT")
            return {
                "tampered":   False,
                "status":     "intact",
                "record_id":  req.record_id,
                "file_hash":  current["file_hash"],
                "total_rows": current["total_rows"],
                "message":    "Data verified. Safe to proceed."
            }

        col_status    = {
            col: "ok" if current["col_hashes"][col] == stored_col_hashes[col] else "tampered"
            for col in ["temperature", "latitude", "longitude"]
        }
        tampered_cols = [c for c, s in col_status.items() if s == "tampered"]
        log.warning(f"[VERIFY] TAMPERED — columns: {tampered_cols}")

        return {
            "tampered":    True,
            "status":      "tampered",
            "record_id":   req.record_id,
            "file_hash":   current["file_hash"],
            "stored_hash": stored_file_hash,
            "total_rows":  current["total_rows"],
            "columns":     col_status,
            "message":     f"TAMPER DETECTED in: {', '.join(tampered_cols)}. Do not proceed."
        }

    except Exception as e:
        log.error(f"[VERIFY] {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/record/{record_id}")
def get_record(record_id: int):
    """Read a stored record from chain. Free — no gas needed."""
    try:
        _, contract, _ = _connect()
        r = contract.functions.getRecord(record_id).call()
        return {
            "record_id":   record_id,
            "file_name":   r[0],
            "file_hash":   "0x" + r[1].hex(),
            "col_hashes": {
                "temperature": "0x" + r[2].hex(),
                "latitude":    "0x" + r[3].hex(),
                "longitude":   "0x" + r[4].hex(),
            },
            "total_rows":    r[5],
            "registered_at": r[6],
            "registered_by": r[7],
        }
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))
