import pickle

import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

from eth_fraud_detection.adapters.postgresql_db import PostgresSQLDb
from eth_fraud_detection.utils.logger import eth_logger

FEATURES = [
    "value_eth",
    "gasPrice_gwei",
    "nonce",
    "out_degree",
    "in_degree",
    "unique_counterparties",
    "total_volume",
    "avg_tx_value",
]

# Fraction of the dataset expected to be anomalous.
# Tune this based on domain knowledge or labelled samples.
CONTAMINATION = 0.05


def _to_matrix(records: list[dict]) -> np.ndarray:
    return np.array([[r[f] or 0.0 for f in FEATURES] for r in records], dtype=float)


async def run_isolation_forest(
    contamination: float = CONTAMINATION,
    batch_size: int = 10_000,
) -> None:
    db = PostgresSQLDb()
    await db.connect()
    try:
        records = await db.get_unscored(batch_size=batch_size)
        if not records:
            eth_logger.info("No unscored transactions — skipping Isolation Forest run.")
            return

        X = _to_matrix(records)

        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)

        model = IsolationForest(contamination=contamination, random_state=42, n_jobs=-1)
        model.fit(X_scaled)

        with open('isolated_tree.pkl', 'wb') as file:
            pickle.dump(model, file)

        raw_scores = -model.score_samples(X_scaled)
        predictions = model.predict(X_scaled)

        threshold = float(np.percentile(raw_scores, (1 - contamination) * 100))

        n_fraud = int((predictions == -1).sum())
        eth_logger.info(
            f"Isolation Forest scored {len(records)} transactions — "
            f"{n_fraud} flagged as fraud (threshold={threshold:.4f})."
        )
    finally:
        await db.close()