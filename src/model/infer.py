from __future__ import annotations

import os
import torch
import numpy as np

from typing import List, Dict, Tuple
from transformers import AutoTokenizer, AutoModel, AutoModelForSequenceClassification


def _pick_device():
    forced = os.getenv("TORCH_DEVICE")
    if forced in {"cpu", "cuda", "mps"}:
        return torch.device(forced)
    # Mặc định ưu tiên CPU để ổn định trên macOS
    return torch.device("cpu")
    # (Sau khi ổn bạn có thể bật lại auto: cuda > mps > cpu)


class InferenceService:
    def __init__(
        self, embed_model: str, clf_model: str, pos_idx: int = 1, max_len: int = 256
    ):
        self.device = _pick_device()
        # Embedding
        self.emb_tok = AutoTokenizer.from_pretrained(embed_model)
        self.emb_mdl = AutoModel.from_pretrained(embed_model).to(self.device).eval()
        # Classifier
        self.clf_tok = AutoTokenizer.from_pretrained(clf_model)
        self.clf_mdl = (
            AutoModelForSequenceClassification.from_pretrained(clf_model)
            .to(self.device)
            .eval()
        )
        self.pos_idx = pos_idx
        self.max_len = max_len

    @torch.inference_mode()
    def embed(self, texts: List[str], batch_size: int = 64) -> List[List[float]]:
        import torch.nn.functional as F

        if not texts:
            return []
        outs = []
        for i in range(0, len(texts), batch_size):
            batch = texts[i : i + batch_size]
            enc = self.emb_tok(
                batch,
                padding=True,
                truncation=True,
                max_length=self.max_len,
                return_tensors="pt",
            ).to(self.device)
            last = self.emb_mdl(**enc).last_hidden_state  # [B,T,H]
            mask = enc["attention_mask"].unsqueeze(-1).float()
            pooled = (last * mask).sum(1) / mask.sum(1).clamp(min=1e-9)
            pooled = F.normalize(pooled, p=2, dim=1)
            outs.append(pooled.cpu().numpy())
        if not outs:
            return []
        arr = np.vstack(outs)  # [N, H]
        return [row.astype(float).tolist() for row in arr]

    @torch.inference_mode()
    def classify(self, texts: List[str], batch_size: int = 64) -> List[float]:
        if not texts:
            return []
        outs = []
        for i in range(0, len(texts), batch_size):
            batch = texts[i : i + batch_size]
            enc = self.clf_tok(
                batch,
                padding=True,
                truncation=True,
                max_length=self.max_len,
                return_tensors="pt",
            ).to(self.device)
            logits = self.clf_mdl(**enc).logits.cpu().numpy()  # [B,C]
            x = logits - logits.max(axis=-1, keepdims=True)
            probs = np.exp(x) / np.exp(x).sum(axis=-1, keepdims=True)
            idx = self.pos_idx if probs.shape[1] > self.pos_idx else -1
            outs.append(probs[:, idx])
        if not outs:
            return []
        return np.concatenate(outs).astype(float).tolist()


# ---- Executor-local singleton (mỗi worker 1 instance) ----
_SINGLETONS: Dict[Tuple[str, str, int, int], InferenceService] = {}


def get_service(
    embed_model: str, clf_model: str, pos_idx: int = 1, max_len: int = 256
) -> InferenceService:
    key = (embed_model, clf_model, pos_idx, max_len)
    svc = _SINGLETONS.get(key)
    if svc is None:
        svc = InferenceService(embed_model, clf_model, pos_idx, max_len)
        _SINGLETONS[key] = svc
    return svc
