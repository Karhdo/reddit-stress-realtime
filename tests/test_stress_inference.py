from __future__ import annotations

import pytest

from src.model.infer import get_service
from src.common.config import load_config


@pytest.mark.infer
def test_single_text_stress_score():
    """
    Business test:
    - Input: a real-world stress-related text
    - Output: stress probability in [0, 1]
    """

    cfg = load_config()

    svc = get_service(
        clf_model=cfg.model.classification_model,
        pos_idx=int(cfg.model.classifier_label_index),
        max_len=int(getattr(cfg.model, "max_len", 256)),
        cfg=cfg,
    )

    text = 'He said he had not felt that way before, suggeted I go rest and so ..TRIGGER AHEAD IF YOUI\'RE A HYPOCONDRIAC LIKE ME: i decide to look up "feelings of doom" in hopes of maybe getting sucked into some rabbit hole of ludicrous conspiracy, a stupid "are you psychic" test or new age b.s., something I could even laugh at down the road. No, I ended up reading that this sense of doom can be indicative of various health ailments; one of which I am prone to.. So on top of my "doom" to my gloom..I am now f\'n worried about my heart. I do happen to have a physical in 48 hours.'

    scores = svc.classify([text], batch_size=1)

    assert len(scores) == 1

    score = scores[0]

    # ---- Assertions ----
    assert isinstance(score, float)
    assert 0.0 <= score <= 1.0

    # ---- Logging for human inspection ----
    print(f"\n🧠 Stress score = {score:.4f} ({score * 100:.2f}%)")

    # Optional: sanity threshold (adjust if needed)
    assert score > 0.3, "Expected stressed text to have non-trivial stress score"
