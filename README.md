# resourcesync-oai-pmh

This is a collection of wrapper scripts for easy setup and use of both sides (source and destination) of the [ResourceSync web synchronization framework](http://www.openarchives.org/rs/resourcesync) with existing [OAI-PMH](https://www.openarchives.org/pmh/) metadata providers on the source side and a Solr index (in addition to a scheduling server) on the destination side. This solution was developed for UCLA's PRRLA project. For project information, see [our wiki](https://docs.library.ucla.edu/display/dlp/PRRLA+%28Pacific+Rim+Research+Libraries+Alliance%29+Project+Overview) or http://pr-rla.org.

## Installation

### Source

1. Install Python 3.5 or greater.
2. Download `oaipmh-rs-src.py`.
3. Install its singular dependency by following the instructions [here](https://github.com/resourcesync/py-resourcesync#installation-from-source) and [here](https://github.com/resourcesync/py-resourcesync#installation).

### Destination

1. Install Python 3.4 or greater and `cron`.
2. Download `oaipmh-rs-dest.py`.
3. Install dependencies:
   ```bash
   pip3 install -r oaipmh-rs-dest-requirements.txt
   ```
4. Setup a TinyDB instance (usually local) and a Solr index (usually remote) for the script.
5. Create a `cron` job to schedule the script for execution.

## Usage

See the [wiki](https://github.com/UCLALibrary/resourcesync-oai-pmh/wiki) for recipes.
