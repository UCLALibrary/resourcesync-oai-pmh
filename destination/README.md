# Installation

1. Install Python 3.4 or greater and `cron`.
2. Download `destination.py`.
3. Install dependencies:
   ```bash
   pip3 install -r destination_requirements.txt
   ```
4. Edit `destination.ini` with the correct values for each key, and edit `destination_logging.ini` to point to the path of the output file to log to.
5. Setup a TinyDB instance (usually local) according to the [schema](https://github.com/UCLALibrary/resourcesync-oai-pmh/wiki/Table-Schemas#destination) and a Solr 4.10 index (usually remote).
6. Create a `cron` job to schedule the script for execution.

## Usage

```bash
python3 destination.py
```

## Examples

See the [wiki](https://github.com/UCLALibrary/resourcesync-oai-pmh/wiki).
