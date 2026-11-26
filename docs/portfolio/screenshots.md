# Screenshots

The eight console frames live in [`docs/assets`](../assets). They were captured on 2026-11-23 from the development console at `http://localhost:3000`, viewport 1440×1100, after each page left its loading state. Playwright Chromium loaded the pages. No image was drawn or substituted.

The development volumes were already populated. This capture did not reset them and did not publish a new snapshot. `GET /api/v1/warehouse/status` at capture time reported `ready: true` for warehouse publication `50d22fe5-8db0-4f2f-bc00-47bda48152fc`, Silver `7e8d34e7-c701-4084-8d56-edc040a5fe59`, 362 accepted orders, 540 items, 193 payments, and 175 inventory events. Revenue stayed separate: EUR 18711.58, GBP 16558.03, USD 17768.77. The forecast label on screen was `seasonal_naive`. Freshness of about 16 hours is the wall-clock age of the 2026-11-22T17:35:14Z publication, not a new pipeline run.

| File | Route | Heading confirmed on the page |
| --- | --- | --- |
| [overview.png](../assets/overview.png) | `/` | Operations overview |
| [warehouse.png](../assets/warehouse.png) | `/warehouse` | Warehouse analytics |
| [pipelines.png](../assets/pipelines.png) | `/pipelines` | Pipeline orchestration |
| [ml.png](../assets/ml.png) | `/ml` | Demand intelligence |
| [data-quality.png](../assets/data-quality.png) | `/data-quality` | Data quality |
| [monitoring.png](../assets/monitoring.png) | `/monitoring` | System monitoring |
| [streaming.png](../assets/streaming.png) | `/streaming` | Event streaming |
| [data-sources.png](../assets/data-sources.png) | `/data-sources` | Data sources |

The frames show the publication id, separate currency totals, and `seasonal_naive`. They do not show a token or a password. A later `scripts/demo_day7.py` run replaces the publication id; recapture after that run instead of reusing these files.

## v0.2.0 acceptance capture

On 2026-11-26 the redesigned console was reviewed live at `http://localhost:23000`.
All eight routes rendered real API data from the local publication. The responsive
audit covered 1440×900, 1024×768, 768×1024, and 390×844 with zero axe violations;
the narrow view confirmed the mobile header, stacked KPI cards, and collapsible
navigation. This review is acceptance evidence for the redesign candidate and is
not a release publication.
