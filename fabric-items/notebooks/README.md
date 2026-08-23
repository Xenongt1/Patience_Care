# Notebooks

Fabric Notebooks (Spark Structured Streaming):

- `archive_writer_vitals.ipynb`, `archive_writer_prescriptions.ipynb` — Bronze archival, consuming
  from each Event Hub's dedicated archive consumer group and writing gzipped JSONL to ADLS Gen2.
  This is the archive-writer mechanism decided on for this project (Fabric Notebook, not an
  Azure Function) to keep it inside the Fabric estate.
- `reference_snapshot_loader.ipynb` — daily push of batch Gold dimensions/facts into the
  Eventhouse `ref_*` tables (may instead live as a Data Pipeline activity — see `../pipelines/`).

Not yet written.
