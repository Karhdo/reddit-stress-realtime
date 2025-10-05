from src.model.infer import StressClassifier


def test_infer_runs():
    clf = StressClassifier()
    label, score = clf.predict_label("I feel overwhelmed and anxious about work")
    assert label in {"stress", "non-stress"}
    assert 0.0 <= score <= 1.0
