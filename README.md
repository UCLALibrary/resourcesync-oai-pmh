# resourcesync-oai-pmh

This is a collection of wrapper scripts for easy setup and use of both sides (source and destination) of the [ResourceSync web synchronization framework](http://www.openarchives.org/rs/resourcesync) with existing [OAI-PMH](https://www.openarchives.org/pmh/) metadata providers on the source side and a Solr index (in addition to a scheduling server) on the destination side. This solution was developed for UCLA's PRRLA project. For project information, see [our wiki](https://docs.library.ucla.edu/display/dlp/PRRLA+%28Pacific+Rim+Research+Libraries+Alliance%29+Project+Overview) or http://pr-rla.org.

## Installation

Depending on your institution's role in the ResourceSync framework, you'll pick one of the following two sets of setup instructions:

### Content provider (source)

1. Set up a server according to [these specifications](https://github.com/UCLALibrary/resourcesync-oai-pmh/wiki/Source-Server-Specs).
2. Install Python 3.5 or greater and a web server of your choosing (for serving static files).
3. Download `rs_oaipmh_src.py`.
4. Install its singular dependency by following the instructions [here](https://github.com/resourcesync/py-resourcesync#installation-from-source) and [here](https://github.com/resourcesync/py-resourcesync#installation).
5. Generate some ResourceSync documents and serve them up! Get started by visiting the [Usage](https://github.com/UCLALibrary/resourcesync-oai-pmh#rs_oaipmh_srcpy) and [Examples](https://github.com/UCLALibrary/resourcesync-oai-pmh#examples) sections below.

### Content aggregator (destination)

1. Install Python 3.4 or greater and `cron`.
2. Download `rs_oaipmh_dest.py`.
3. Install dependencies:
   ```bash
   pip3 install -r rs_oaipmh_dest_requirements.txt
   ```
4. Setup a TinyDB instance (usually local) according to the [schema](https://github.com/UCLALibrary/resourcesync-oai-pmh/wiki/Table-Schemas#rs_oaipmh_destpy) and a Solr 4.10 index (usually remote).
5. Create a `cron` job to schedule the script for execution.

## Usage

### rs_oaipmh_src.py

This script has two sub-commands:

#### `single`

Creates, modifies, deletes, or otherwise updates ResourceSync documents for a **single** set of metadata records. Parameters are specified on the command line. For details, run:
```bash
python3 rs_oaipmh_src.py single --help
```

#### `multi`

Creates, modifies, deletes, or otherwise updates ResourceSync documents for **multi**ple (one or more) sets of metadata records.  Parameters are specified in a CSV file that is passed to the program (use the example file in this repository as a template, according to [this schema](https://github.com/UCLALibrary/resourcesync-oai-pmh/wiki/Table-Schemas#rs_oaipmh_srcpy)). For details, run:
```bash
python3 rs_oaipmh_src.py multi --help
```

### rs_oaipmh_dest.py

For detailed usage instructions, run:
```bash
python3 rs_oaipmh_dest.py --help
```

### Examples

See the [wiki](https://github.com/UCLALibrary/resourcesync-oai-pmh/wiki).

## Tests

To run automated tests, do:
```bash
python3 -m unittest discover -s test # unless you're a developer, you shouldn't need to do this

```
