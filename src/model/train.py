"""Minimal example training loop.
In practice, replace the toy dataset with your labeled data.
"""

import os
from datasets import load_dataset
from transformers import (
    AutoTokenizer,
    AutoModelForSequenceClassification,
    TrainingArguments,
    Trainer,
)
import numpy as np
from sklearn.metrics import accuracy_score, f1_score

MODEL_NAME = os.getenv("MODEL_NAME", "distilbert-base-uncased")
ARTIFACT = os.getenv("MODEL_ARTIFACT", "src/model/artifacts/stress-classifier")


def compute_metrics(eval_pred):
    logits, labels = eval_pred
    preds = np.argmax(logits, axis=-1)
    return {"accuracy": accuracy_score(labels, preds), "f1": f1_score(labels, preds)}


def main():
    # Example: use a sentiment dataset as proxy; replace with your labeled stress dataset
    ds = load_dataset("imdb")
    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)

    def tok(batch):
        return tokenizer(batch["text"], truncation=True, padding=True, max_length=256)

    ds = ds.map(tok, batched=True)
    ds = ds.rename_column("label", "labels")
    ds.set_format(type="torch", columns=["input_ids", "attention_mask", "labels"])

    model = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME, num_labels=2)

    args = TrainingArguments(
        output_dir="/tmp/out",
        evaluation_strategy="epoch",
        per_device_train_batch_size=8,
        per_device_eval_batch_size=8,
        num_train_epochs=1,
        fp16=False,
        logging_steps=50,
        save_strategy="epoch",
        report_to=[],
    )

    trainer = Trainer(
        model=model,
        args=args,
        train_dataset=ds["train"].select(range(2000)),
        eval_dataset=ds["test"].select(range(1000)),
        compute_metrics=compute_metrics,
    )
    trainer.train()

    model.save_pretrained(ARTIFACT)
    tokenizer.save_pretrained(ARTIFACT)
    print(f"Saved to {ARTIFACT}")


if __name__ == "__main__":
    main()
