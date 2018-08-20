#!/usr/bin/python3

import argparse
from configparser import ConfigParser
import csv
from json import dumps
import logging
import os
from resourcesync.resourcesync import ResourceSync, Parameters
from resourcesync.generators.oaipmh_generator import OAIPMHGenerator

def main():

    parser = argparse.ArgumentParser(description='Generate sitemaps for a ResourceSync source server.')
    subparsers = parser.add_subparsers(title='commands', metavar='COMMAND', description='Each command specifies a different mode for generating sitemaps. For detailed usage instructions, run `python3 source.py COMMAND -h`.')


    ### Subcommand - single sitemap
    parser_a = subparsers.add_parser('single', description='Generate a single sitemap by specifying parameters on the command line.', help='generate a single sitemap')
    parser_a.set_defaults(command='single')

    # ResourceSync server
    parser_a.add_argument('resourcesync-server-hostname', metavar='<resourcesync-server-hostname>', help='hostname of the ResourceSync source server')
    parser_a.add_argument('resourcesync-server-document-root', metavar='<resourcesync-server-document-root>', help='document root of the server (pass "apache" for "/var/www/html", or "tomcat" for "/var/lib/tomcat/webapps/default")')
    parser_a.add_argument('--resource-dir', metavar='<path>', help='path to a directory under <resourcesync-server-document-root> where <metadata-dir>s will be put for each collection (if unspecified, defaults to "/resourcesync")')

    # OAI-PMH data provider
    parser_a.add_argument('oaipmh-base-url', metavar='<oaipmh-base-url>', help='OAI-PMH base URL to which query parameters are appended')
    parser_a.add_argument('oaipmh-set', metavar='<oaipmh-set>', help='name of the collection of resources to generate capability documents for; must be a valid name for a directory in your web server\'s document root')
    parser_a.add_argument('oaipmh-metadataprefix', metavar='<oaipmh-metadataprefix>', choices=['oai_dc','mods'], help='"oai_dc" or "mods"')
    parser_a.add_argument('strategy', metavar='<strategy>', choices=['resourcelist', 'new_changelist', 'inc_changelist'], help='"resourcelist", "new_changelist", or "inc_changelist"')

    ### Subcommand - multiple sitemaps
    parser_b = subparsers.add_parser('multi', description='Generate multiple sitemaps by specifying parameters as rows in a CSV.', help='generate multiple sitemaps')
    parser_b.set_defaults(command='multi')
    parser_b.add_argument('collections-csv', metavar='<collections-csv>', help='path to file containing information for each collection to process')


    args = vars(parser.parse_args())


    base_dir = os.path.abspath(os.path.dirname(__file__))
    '''
    # not currently being used
    config_path = os.path.join(base_dir, 'source.ini')
    config = ConfigParser()
    config.read(config_path)
    '''
    logging_config_path = os.path.join(base_dir, 'source_logging.ini')
    logging_config = ConfigParser()
    logging_config.read(logging_config_path)

    logging.config.fileConfig(logging_config_path)
    logger = logging.getLogger('root')

    logger.info('--- STARTING RUN ---')
    logger.info('')
    logger.info('Logging to {}'.format(os.path.abspath(os.path.expanduser(logging_config['DEFAULT']['logfile_path']))))

    logger.debug('Program arguments:')
    logger.debug('')

    out = dumps(args, indent=4)
    for line in out.splitlines():
        logger.debug(line)
    logger.debug('')


    common_document_roots = {
        'apache': '/var/www/html',
        'tomcat': '/var/lib/tomcat/webapps/default'
    }

    collections = []

    if args['command'] == 'single':
        collection = {}
        collection['resourcesync_url'] = args['resourcesync-server-hostname']
        collection['strategy'] = args['strategy']

        # some logic to set default values
        collection['document_root'] = common_document_roots[args['resourcesync-server-document-root']] if args['resourcesync-server-document-root'] in common_document_roots else args['resourcesync-server-document-root']
        collection['resource_dir'] = args['resource_dir'] if args['resource_dir'] is not None else 'resourcesync'

        collection['oaipmh_base_url'] = args['oaipmh-base-url']
        collection['oaipmh_set'] = args['oaipmh-set']
        collection['oaipmh_metadataprefix'] = args['oaipmh-metadataprefix']

        collections.append(collection)

    elif args['command'] == 'multi':
        try:
            with open(args['collections-csv'], 'r') as f:
                csvreader = csv.DictReader(f, delimiter=',', quotechar='|')
                try:
                    for row in csvreader:
                        collection = {}
                        collection['resourcesync_url'] = row['resourcesync-server-hostname']
                        collection['strategy'] = row['strategy']

                        # some logic to set default values
                        collection['document_root'] = common_document_roots[row['resourcesync-server-document-root']] if row['resourcesync-server-document-root'] in common_document_roots else row['resourcesync-server-document-root']
                        collection['resource_dir'] = row['resource-dir'] if row['resource-dir'] is not '' else 'resourcesync'

                        collection['oaipmh_base_url'] = row['oaipmh-base-url']
                        collection['oaipmh_set'] = row['oaipmh-set']
                        collection['oaipmh_metadataprefix'] = row['oaipmh-metadataprefix']

                        collections.append(collection)
                except csv.Error as e:
                    logger.critical('File {}, line {}: {}'.format(args['collections-csv'], reader.line_num, e))
                    exit(1)
        except:
            logger.critical('Invalid command line argument: {} does not exist'.format(args['collections-csv']))
            exit(1)

    logger.debug('Collection-specific parameters:')
    logger.debug('')

    out = dumps(collections, indent=4)
    for line in out.splitlines():
        logger.debug(line)
    logger.debug('')

    if len(collections) == 0:
        logger.error('no collections to process')
        exit(1)

    else:
        for collection in collections:
            try:
                my_generator = OAIPMHGenerator(params={
                    'oaipmh_base_url':       collection['oaipmh_base_url'],
                    'oaipmh_set':            collection['oaipmh_set'],
                    'oaipmh_metadataprefix': collection['oaipmh_metadataprefix']})

                rs = ResourceSync(generator=my_generator,
                                  strategy=collection['strategy'],
                                  resource_dir='{}/{}'.format(collection['document_root'], collection['resource_dir']),
                                  metadata_dir=collection['oaipmh_set'],
                                  description_dir=collection['document_root'],
                                  document_root=collection['document_root'],
                                  url_prefix='{}/{}'.format(collection['resourcesync_url'], collection['resource_dir']),
                                  is_saving_sitemaps=True)
                rs.execute()
            except Exception as e:
                logger.error('Unable to generate "{}" for collection "{}": {}'.format(
                    collection['strategy'],
                    collection['oaipmh_set'],
                    e))

    logger.info('')
    logger.info('---  ENDING RUN  ---')

if __name__ == '__main__':
    main()
