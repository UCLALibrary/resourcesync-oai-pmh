# Installation

1. Set up a server according to [these specifications](https://github.com/UCLALibrary/resourcesync-oai-pmh/wiki/Source-Server-Specs).
2. Install Python 3.5 or greater and a web server of your choosing (for serving static files).
3. Download all the files in this directory (`source.py`, `source.ini`, etc.).
4. Install dependencies by following the instructions [here](https://github.com/resourcesync/py-resourcesync#installation-from-source) and [here](https://github.com/resourcesync/py-resourcesync#installation).
5. Edit `source.ini` to point to the path of `source_logging.ini`, and edit `source_logging.ini` to point to the path of the output file to log to. NOTE: you may need to run the Python script with elevated privileges, so be careful with paths that include `~`!
6. Generate some ResourceSync documents and serve them up! Get started by visiting the Usage and Examples sections below.

# Usage

`source.py` has two sub-commands:

## `single`

Creates, modifies, deletes, or otherwise updates ResourceSync documents for a **single** set of metadata records. Parameters are specified on the command line. For details, run:
```bash
python3 source.py single --help
```

## `multi`

Creates, modifies, deletes, or otherwise updates ResourceSync documents for **multi**ple (one or more) sets of metadata records.  Parameters are specified in a CSV file that is passed to the program (use the example file in this repository as a template, according to [this schema](https://github.com/UCLALibrary/resourcesync-oai-pmh/wiki/Table-Schemas#source)). For details, run:
```bash
python3 source.py multi --help
```

## Examples

See the [wiki](https://github.com/UCLALibrary/resourcesync-oai-pmh/wiki).
