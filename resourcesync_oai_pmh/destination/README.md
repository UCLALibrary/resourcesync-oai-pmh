# Installation

1. Install cron, Python 3.4 or greater, and Git (optional).
2. Download and extract this repository to your system. You'll be using the files in `resourcesync_oai_pmh/destination`.
3. Set up your working directory:
    ```bash
    cp resourcesync-oai-pmh/resourcesync_oai_pmh/destination/destination.ini .
    mkdir ~/thumbnails
    touch ~/db.json
    ```
4. If setting up a development environment, install a Python 3 virtual environment manager and create an environment:
    ```bash
    # Ubuntu 16.04
    sudo apt-get install python3-venv
    python3 -m venv prrla-venv
    ```

    ```bash
    # OSX
    pip3 install virtualenv
    python3 -m virtualenv prrla-venv
    ```

    Fire it up:
    ```bash
    source prrla-venv/bin/activate
    ```
5. Install Python dependencies:
    ```bash
    pip3 install -r resourcesync-oai-pmh/resourcesync_oai_pmh/destination/destination_requirements.txt
    ```
    On OSX: if `lxml` fails to install, you may need to install it with `STATIC_DEPS` set to `true` per https://lxml.de/installation.html.
6. Add a profile for the thumbnail S3 bucket (be sure to specify the bucket region):
    ```bash
    aws configure --profile my-thumbnails-profile-name
    ```
7. Setup a TinyDB instance (usually local) according to the [schema](https://github.com/UCLALibrary/resourcesync-oai-pmh/wiki/Table-Schemas#destination) and a Solr 4.10 index (usually remote) by adding a row for each collection. Populate the database using the `PRRLATinyDB` class found in `resourcesync-oai-pmh/resourcesync_oai_pmh/destination/util.py`.
8. Edit `./destination.ini` with the correct values for each key:
    - `S3.bucket`: hostname identifier for the S3 bucket
    - `S3.profile_name`: S3 profile name passed to `aws configure` in step 4 above
    - `S3.thumbnail_dir`: location for writing local copies of thumbnails (`~/thumbnails`)
    - `Solr.url`: base URL for the Solr index
    - `TinyDB.path`: location of the internal database (`~/db.json`)
9. Copy `./destination.ini` back to its original location:
    ```bash
    cp ./destination.ini resourcesync-oai-pmh/resourcesync_oai_pmh/destination/destination.ini
    ```
10. Edit `resourcesync-oai-pmh/resourcesync_oai_pmh/destination/destination_logging.ini` to change `logfile_path`, if desired.
11. Create a `cron` job to schedule the script for execution (optional).

# Usage

```bash
python3 destination.py
```

# Tests

To run automated tests, do:
```bash
python3 -m unittest discover -s test
```
