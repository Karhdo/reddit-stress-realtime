from dataclasses import dataclass


@dataclass
class ModelCfg:
    # Embedding Model Config
    embedding_model: str = "sentence-transformers/all-MiniLM-L6-v2"
    batch_size: int = 16
    feature_version: str = "v1"

    # Classification Model Config
    classification_model: str = "distilbert-base-uncased-finetuned-sst-2-english"
    classifier_label_index: int = (
        1  # Index of the positive label in the model's output logits
    )
    classifier_batch_size: int = 16
    classifier_threshold: float = 0.6
    model_version: str = "sst2-v1"
