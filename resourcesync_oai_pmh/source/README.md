# Installation

1. Set up a server according to [these specifications](https://github.com/UCLALibrary/resourcesync-oai-pmh/wiki/Source-Server-Specs).
2. Install Python 3.5 or greater and a web server of your choosing (for serving static files).
3. Install dependencies by following the instructions [here](https://github.com/resourcesync/py-resourcesync#installation-from-source) and [here](https://github.com/resourcesync/py-resourcesync#installation).
4. Download and extract this repository to your system. You'll be using the files in `resourcesync_oai_pmh/source`.
5. This software should "just work" without requiring any modification of any files, as long as the directory structure remains the same. You may want to modify the value of [DEFAULT]->[logfile_path] in `source_logging.ini`.
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

Please read [this wiki page](https://github.com/UCLALibrary/resourcesync-oai-pmh/wik://github.com/UCLALibrary/resourcesync-oai-pmh/wiki/Use-Case-Recipes).

# Further Information

Please read [this wiki page](https://github.com/UCLALibrary/resourcesync-oai-pmh/wiki/Further-Information-and-Considerations-for-Content-Providers).
