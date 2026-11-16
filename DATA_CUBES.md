# SDA data cubes

Choose **Data Cubes** in Dataset Focus. The global reporting quarter and state selection apply; cube-specific filters and heatmap axes are restricted to dimensions actually present in the selected file. There are 14 preset views, chart-value tables, PNG export and CSV export (including the displayed growth/percentage calculations and source metadata).

The initial five uploaded files cover December 2024–September 2026 except pipeline, which covers June and September 2026. These sources are kept separate from Supplement P, including the observed June 2026 pipeline and March 2025 participant-status discrepancies.

## Releases and data rules

`cube_store.py` stores five separate aggregate fact tables at the full published source grain, immutable source files, SHA256 checksums and release metadata. The bundled compressed CSVs are loaded transactionally on startup without changing Supplement P measures. Restarting does not duplicate a release. Raw source dimensions and tokens are preserved alongside reviewed display labels.

Use **Upload Data → Upload an SDA Data Cube Release** to add all five CSV files together. All five schemas, reporting dates, counts and complete dimension keys are validated before a release is committed. Previous releases remain selectable. Release coverage reports new dates and changed/added/removed historical source cells. A changed category label can appear as an added/removed cell; this is a source revision indicator, not evidence of individual movements.

- Sum count measures within one snapshot; do not count CSV rows or sum snapshots as a cohort total.
- Never join the raw participant fact tables or add their populations together.
- An aggregate containing suppressed/unpublished contributions has no numeric value. Missing matrix combinations stay unavailable. Unfiltered demographic SIL aggregation requires both source-code contributions.
- Shares require a complete displayed denominator. Keep unknown/missing categories visible.
- Heatmap colour domains use the full available history for the same selection. High-cardinality selections require narrower filters before charting; their data remains exportable.
- Pipeline entry years are not completion dates. Missing/1899/future years remain in a reviewed unverified cohort.
- Net enrolled growth requires consecutive snapshots; it differs from the published last-quarter flag. Maximum residents is not occupancy or vacancy.

## API

All cube endpoints require the dashboard session; uploads require the admin session.

- `GET /api/cubes[?release_id=...]`: coverage, valid dimensions, releases and source-revision counts.
- `GET /api/cubes/<cube>/filters?release_id=...`: published filter values.
- `POST /api/cubes/<cube>/query`: `release_id`, calendar `quarter` (e.g. `2026-Q3`), one or two `group_by` dimensions and optional `filters`.
- `POST /api/cubes/<cube>/export`: aggregate source rows with value status and provenance. The UI additionally exports derived chart values such as quarterly growth and percentages.
- `POST /admin/cubes/upload`: five `files` CSV parts and a human-readable `label`.

## Validation

Run `python -m unittest discover -s tests -p 'test_*.py'` and the existing `node tests/enrolled-participant.cjs` / `node tests/detailed-building.cjs` checks. The optional workbook integration test requires `SDA_TEST_WORKBOOK`.

Browser validation used the real Flask responses and Plotly 2.27.0 in Chromium: all existing focuses at 1500, 1024, 768 and 390 pixels; all five cubes and 14 presets; actual CSV downloads; global quarter/state changes; missing snapshots; percentages; and the five-file admin form. The chart sizing fix constrains flexible grid tracks and observes plot container resizes, including hidden-to-visible transitions.
