from dataclasses import dataclass


@dataclass
class ModelCfg:
    """
    Unified model configuration for multi-language stress detection.
    Supports:
      - Vietnamese model (PhoBERT)
      - English model (DistilBERT)
      - Fallback model (default: PhoBERT)
    """

    # ===== Embedding (if needed later) =====
    embedding_model: str = "sentence-transformers/all-MiniLM-L6-v2"
    batch_size: int = 16
    feature_version: str = "v1"

    # ===== Multi-language Classification =====

    # VI → PhoBERT .zip file (usually loaded from MinIO)
    classification_model_vi: str = (
        "s3a://artifacts/models/20251130T133740Z/phobert_finetuned.zip"
    )

    # EN → DistilBERT .zip file (English model)
    classification_model_en: str = (
        "s3a://artifacts/models/20251104T073104Z/distilbert_finetuned.zip"
    )

    # Fallback (nếu lang detection fail)
    classification_model: str = (
        "s3a://artifacts/models/20251130T133740Z/phobert_finetuned.zip"
    )

    # Which index represents the positive class
    classifier_label_index: int = 1

    # Batch size for transformer inference
    classifier_batch_size: int = 16

    # Threshold for label_stress = score >= threshold
    classifier_threshold: float = 0.5

    # Max sequence length for tokenizer
    max_len: int = 256

    # Version tag that will appear in Gold table
    model_version: str = "phobert-distilbert-combo-finetuned-v1"
