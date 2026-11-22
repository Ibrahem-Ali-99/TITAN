"""Synthetic order event generator.

The seed fixes the business payload and occurred_at timeline. Event identities
stay random unless ``deterministic_event_ids`` is requested. Generated events
are labeled with source ``synthetic.event-generator``.
"""

from event_generator.generator import generate_orders

__all__ = ["generate_orders"]
