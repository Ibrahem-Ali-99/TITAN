"""Choose the served forecast from one held-out comparison.

The candidate and the seasonal naive see the same test rows. A worse candidate
is retained with its metrics and artifact. It is not the served method.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class ServingChoice:
    strategy: str
    rationale: str


def choose_strategy(mae_model: float, mae_naive: float) -> ServingChoice:
    """Prefer the baseline when its MAE is lower or equal. Do not retune on the test week."""
    if mae_model < mae_naive:
        return ServingChoice(
            strategy="hist_gradient_boosting",
            rationale=(
                "The candidate MAE is lower on the same held-out dates. "
                "Those dates were not used to retune the model."
            ),
        )
    return ServingChoice(
        strategy="seasonal_naive",
        rationale=(
            "The seasonal naive MAE is lower or equal on the held-out dates. "
            "The candidate artifact and metrics are retained and are not served."
        ),
    )


def recursive_seasonal(history: list[float], steps: int) -> list[float]:
    """Repeat the value from seven steps earlier. Later steps read prior predictions."""
    values = list(history)
    forecasts: list[float] = []
    for _ in range(steps):
        predicted = values[-7] if len(values) >= 7 else 0.0
        values.append(predicted)
        forecasts.append(predicted)
    return forecasts
