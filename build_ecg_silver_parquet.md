# DICOM Header to Silver Layer Parquet Builder

This script converts DICOM header metadata into a partitioned Parquet dataset with tag-exploded rows.

## Installation

```bash
pip install pydicom pyarrow
```

## Usage

```bash
python build_ecg_silver_parquet.py \
  --dicom_root /mnt/ebs/mimic-iv-ecg-dcm/files \
  --out_dir /mnt/ebs/mimic-iv-ecg-parquet/silver \
  --skip_waveform_data \
  --shard_rows 66000000
```

## Output Structure

The script creates a **partitioned Parquet dataset** organized by `modality` and `tag`:

```
out_dir/
├── modality=MR/
│   ├── tag=00080020/
│   │   ├── part-00000.parquet
│   │   ├── part-00001.parquet
│   │   └── ...
│   ├── tag=00080030/
│   │   └── ...
│   └── ...
├── modality=ECG/
│   └── ...
└── _build_summary.json
```

Each Parquet file contains rows with the following schema:
- `file_path`: Path to the DICOM file
- `study_uid`, `series_uid`, `instance_uid`, `sop_class_uid`: DICOM identifiers
- `modality`: Modality (e.g., MR, ECG)
- `tag`: DICOM tag in hex format (e.g., 00080020)
- `vr`: Value Representation (e.g., DA, LO)
- `vm`: Value Multiplicity (e.g., 1, 1-n)
- `path`: Hierarchical path for nested sequences
- `value`: Tag value as string

## Command-Line Arguments

### Required

- `--dicom_root`: Top-level folder containing `.dcm` files (recursive search)
- `--out_dir`: Output directory for partitioned Parquet dataset

### Optional

- `--shard_rows`: Rows per Parquet file within each partition (default: 2,000,000)
- `--skip_waveform_data`: Exclude Waveform Data tag (5400,1010) - recommended for header-only extraction
- `--extra_skip_tags`: Additional tags to skip (space-separated, e.g., `5400100A` or `5400,100A`)
- `--max_files`: Process only N files (useful for testing)
- `--verbose_every`: Print progress every N files (default: 5000)

## Features

- **Partitioned by modality and tag**: Enables efficient querying by modality or specific tags
- **Tag explosion**: Each DICOM tag becomes a separate row, including nested sequences
- **Multi-value handling**: Multi-valued elements (VM>1) are split into separate rows
- **Memory efficient**: Processes files in batches to control memory usage
- **Resume-friendly**: Can be interrupted and resumed (reprocesses from scratch)

## Example Queries

After building the Parquet dataset, you can query it efficiently:

```python
import pyarrow.dataset as ds
import pyarrow.parquet as pq

# Read specific partition
dataset = ds.dataset("/mnt/ebs/mimic-iv-ecg-parquet/silver", format="parquet")
table = dataset.to_table(filter=ds.field("modality") == "ECG")
df = table.to_pandas()

# Read specific tag across all modalities
table = dataset.to_table(filter=ds.field("tag") == "00080020")
df = table.to_pandas()
```