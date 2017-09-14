# resourcesync-oai-pmh

**NOTE: THIS REPOSITORY IS CURRENTLY UNDER HEAVY DEVELOPMENT! PLEASE CHECK FOR UPDATES REGULARLY, AND PLEASE ONLY USE CODE ON THE `master` BRANCH!**

This is a collection of wrapper scripts for easy setup and use of both sides (source and destination) of the [ResourceSync web synchronization framework](http://www.openarchives.org/rs/resourcesync) with existing [OAI-PMH](https://www.openarchives.org/pmh/) metadata providers on the source side and a Solr index (in addition to a scheduling server) on the destination side. This solution was developed for UCLA's PRRLA project. For project information, see [our wiki](https://docs.library.ucla.edu/display/dlp/PRRLA+%28Pacific+Rim+Research+Libraries+Alliance%29+Project+Overview) or http://pr-rla.org.

Depending on your institution's role in the ResourceSync framework, you'll use one of the two modules in this repository.

## Content provider (source)

If your institution will be providing metadata records for PRRLA, follow the instructions for the [source](resourcesync_oai_pmh/source) module.

## Content aggregator (destination)

If your institution will be aggregating metadata records for PRRLA, follow the instructions for the [destination](resourcesync_oai_pmh/destination) module.

## Tests

To run automated tests, do:
```bash
python3 -m unittest discover -s test # unless you're a developer, you shouldn't need to do this

```
