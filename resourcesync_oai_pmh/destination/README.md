# Installation

1. Install Python 3.4 or greater, `cron`, and the AWS CLI:
  ```bash
  pip3 install awscli
  ```
2. Download and extract this repository to your system. You'll be using the files in `resourcesync_oai_pmh/destination`.
3. Install dependencies:
  ```bash
  pip3 install -r destination_requirements.txt
  ```
4. Add a profile for the thumbnail S3 bucket with `aws configure --profile my-thumbnails-profile-name`.
5. Edit `destination.ini` with the correct values for each key:
  - `S3.bucket`: identifier for the S3 bucket
  - `S3.profile_name`: S3 profile name passed to `aws configure`
  - `Solr.url`: base URL for the Solr index
6. Edit `destination_logging.ini` to change `logfile_path`, if desired.
7. Setup a TinyDB instance (usually local) according to the [schema](https://github.com/UCLALibrary/resourcesync-oai-pmh/wiki/Table-Schemas#destination) and a Solr 4.10 index (usually remote) by adding a row for each collection.
8. Create a `cron` job to schedule the script for execution.

# Usage

```bash
python3 destination.py
```

# Examples

See the [wiki](https://github.com/UCLALibrary/resourcesync-oai-pmh/wiki).
