from __future__ import annotations

import io
import os
import zipfile
import hashlib
import threading
from typing import List, Dict, Tuple, Optional

from src.common.config import Config
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import fsspec  # requires: pip install fsspec s3fs


#  S3 helpers (only what's needed)
def _normalize_s3_uri(uri: str) -> str:
    """Normalize s3a:// -> s3:// for fsspec."""
    return "s3://" + uri[len("s3a://") :] if uri.startswith("s3a://") else uri


def _s3_storage_options(cfg: Optional[Config]) -> dict:
    # Build s3fs options from Config.minio.
    endpoint = getattr(getattr(cfg, "minio", None), "endpoint_url", None) or getattr(
        getattr(cfg, "minio", None), "endpoint", None
    )
    key = getattr(getattr(cfg, "minio", None), "access_key", None)
    secret = getattr(getattr(cfg, "minio", None), "secret_key", None)

    verify = not (endpoint or "").startswith("http://")  # disable TLS verify for http
    return {
        "anon": False,
        "key": key,
        "secret": secret,
        "client_kwargs": {
            "endpoint_url": endpoint,
            "verify": verify,
            "region_name": "us-east-1",
        },
        "config_kwargs": {
            "signature_version": "s3v4",
            "s3": {"addressing_style": "path"},
            "connect_timeout": 30,
            "read_timeout": 60,
        },
    }


# IO utilities
def _assert_exists(uri: str, cfg: Optional[Config]) -> None:
    # Ensure object exists (treat 403 as missing to surface a clear message).
    try:
        fs = fsspec.filesystem("s3", **cfg.s3_storage_options())
        if not fs.exists(_normalize_s3_uri(uri)):
            raise FileNotFoundError(f"Not found: {uri}")
    except Exception as e:
        raise FileNotFoundError(f"Cannot access {uri}: {e}") from e


def _open_bytes(uri: str, cfg: Optional[Config]) -> bytes:
    # Open local or s3(a) file and return bytes.
    if uri.startswith(("s3://", "s3a://")):
        _assert_exists(uri, cfg)
        with fsspec.open(uri, "rb", **cfg.s3_storage_options()) as f:
            return f.read()
    with open(uri, "rb") as f:
        return f.read()


def _extract_zip_to_cache(
    uri: str, cfg: Optional[Config], cache_root: Optional[str] = None
) -> str:
    # Extract a HF model .zip to a content-addressed cache dir and return the folder path.
    raw = _open_bytes(uri, cfg)
    out_hash = hashlib.sha1(raw).hexdigest()[:16]
    cache_dir = cache_root or os.getenv("MODEL_CACHE_DIR", "/tmp/model_cache")
    out_dir = os.path.join(cache_dir, out_hash)
    os.makedirs(out_dir, exist_ok=True)

    cfg_path = os.path.join(out_dir, "config.json")
    if not os.path.isfile(cfg_path):
        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
            zf.extractall(out_dir)
    return out_dir


def _pick_device(cfg: Optional[Config]) -> torch.device:
    # Pick device from Config (cfg.model.torch_device) or default to CPU.
    dev = getattr(getattr(cfg, "model", None), "torch_device", None)
    if dev in {"cpu", "cuda", "mps"}:
        return torch.device(dev)
    return torch.device("cpu")


# Inference service (HF zip only)
class HFZipService:
    """
    Simple inference for exported HuggingFace SequenceClassification model (.zip).
    - classify(texts) -> List[float] of stress probabilities.
    """

    def __init__(
        self,
        zip_uri: str,
        pos_idx: Optional[int] = None,
        max_len: int = 256,
        cfg: Optional[Config] = None,
    ):
        if not (zip_uri and zip_uri.lower().endswith(".zip")):
            raise ValueError(f"Only .zip is supported, got: {zip_uri}")

        self.device = _pick_device(cfg)
        self.max_len = max_len

        model_dir = _extract_zip_to_cache(zip_uri, cfg)
        self.tok = AutoTokenizer.from_pretrained(model_dir, local_files_only=True)
        self.mdl = (
            AutoModelForSequenceClassification.from_pretrained(
                model_dir, local_files_only=True
            )
            .to(self.device)
            .eval()
        )

        self.pos_idx = self._resolve_positive_index(self.mdl, pos_idx)

    @staticmethod
    def _resolve_positive_index(
        model: AutoModelForSequenceClassification, pos_idx: Optional[int]
    ) -> int:
        # Infer positive label index from id2label if not provided.
        if pos_idx is not None and pos_idx >= 0:
            return int(pos_idx)
        cfg = getattr(model, "config", None)
        if cfg is not None and hasattr(cfg, "id2label"):
            id2label = {int(k): str(v).lower() for k, v in cfg.id2label.items()}
            for k, v in id2label.items():
                if "stress" in v or v in {"label_1"}:
                    return int(k)
        return 1  # sensible binary fallback

    @torch.inference_mode()
    def classify(self, texts: List[str], batch_size: int = 64) -> List[float]:
        # Run batched softmax, return probabilities for positive class.
        if not texts:
            return []
        out: List[float] = []
        for i in range(0, len(texts), batch_size):
            enc = self.tok(
                texts[i : i + batch_size],
                padding=True,
                truncation=True,
                max_length=self.max_len,
                return_tensors="pt",
            ).to(self.device)
            logits = self.mdl(**enc).logits
            # numerically-stable softmax
            x = logits - logits.max(dim=-1, keepdim=True).values
            probs = torch.softmax(x, dim=-1)
            idx = self.pos_idx if probs.shape[1] > self.pos_idx else probs.shape[1] - 1
            out.extend(probs[:, idx].detach().cpu().numpy().astype(float).tolist())
        return out

    # Kept for API compatibility
    def embed(self, texts: List[str], batch_size: int = 64) -> List[List[float]]:
        return []


# Worker-local cache
_SINGLETONS: Dict[Tuple[str, int, int], HFZipService] = {}
_LOCK = threading.Lock()


def get_service(
    clf_model: str,  # s3(a)://.../distilbert_finetuned.zip or local .zip
    pos_idx: int = 1,
    max_len: int = 256,
    cfg: Optional[Config] = None,
) -> HFZipService:
    """
    Return a cached HFZipService per (model_uri, pos_idx, max_len).
    Example: s3a://artifacts/models/.../distilbert_finetuned.zip
    """
    if not clf_model or not clf_model.lower().endswith(".zip"):
        raise ValueError(f"Only .zip (HF fine-tuned) is supported. Got: {clf_model}")

    key = (clf_model, pos_idx, max_len)
    with _LOCK:
        svc = _SINGLETONS.get(key)
        if svc is None:
            svc = HFZipService(
                zip_uri=clf_model,
                pos_idx=pos_idx if pos_idx >= 0 else None,
                max_len=max_len,
                cfg=cfg,
            )
            _SINGLETONS[key] = svc
        return svc
