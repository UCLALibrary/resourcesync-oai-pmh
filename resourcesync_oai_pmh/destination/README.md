# Installation

1. Download and extract this repository to your system. You'll be using the files in `resourcesync_oai_pmh/destination`.
2. Set up your working directory:
  ```bash
  cp resourcesync-oai-pmh/resourcesync_oai_pmh/destination/destination.ini .
  mkdir ~/thumbnails
  touch ~/db.json
  ```
3. If setting up a development environment, instantiate a Python virtualenv:
  ```bash
  sudo apt-get install python3-venv
  python3 -m venv prrla-venv
  source prrla-venv/bin/activate
  ```
4. Install cron, Python 3.4 or greater, and packages:
  ```bash
  pip3 install -r resourcesync-oai-pmh/resourcesync_oai_pmh/destination/destination_requirements.txt
  ```
5. Add a profile for the thumbnail S3 bucket (be sure to specify the bucket region):
  ```bash
  aws configure --profile my-thumbnails-profile-name
  ```
6. Setup a TinyDB instance (usually local) according to the [schema](https://github.com/UCLALibrary/resourcesync-oai-pmh/wiki/Table-Schemas#destination) and a Solr 4.10 index (usually remote) by adding a row for each collection. Populate the database using the `PRRLATinyDB` class found in `resourcesync-oai-pmh/resourcesync_oai_pmh/destination/util.py`.
7. Edit `./destination.ini` with the correct values for each key:
  - `S3.bucket`: hostname identifier for the S3 bucket
  - `S3.profile_name`: S3 profile name passed to `aws configure` in step 4 above
  - `S3.thumbnail_dir`: location for writing local copies of thumbnails (`~/thumbnails`)
  - `Solr.url`: base URL for the Solr index
  - `TinyDB.path`: location of the internal database (`~/db.json`)
8. Copy `./destination.ini` back to its original location:
  ```bash
  cp ./destination.ini resourcesync-oai-pmh/resourcesync_oai_pmh/destination/destination.ini
  ```
9. Edit `resourcesync-oai-pmh/resourcesync_oai_pmh/destination/destination_logging.ini` to change `logfile_path`, if desired.
10. Create a `cron` job to schedule the script for execution (optional).

# Usage

```bash
python3 destination.py
```

# Tests

To run automated tests, do:
```bash
python3 -m unittest discover -s test
```