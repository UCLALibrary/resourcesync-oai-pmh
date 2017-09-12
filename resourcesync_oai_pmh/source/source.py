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

    config = ConfigParser()
    config.read('source.ini')

    parser = argparse.ArgumentParser(description='Generate sitemaps for a ResourceSync source server.')
    subparsers = parser.add_subparsers(title='commands', metavar='COMMAND', description='Each command specifies a different mode for generating sitemaps. For detailed usage instructions, run `python3 rs_oaipmh_src.py COMMAND -h`.')


    ### Subcommand - single sitemap
    parser_a = subparsers.add_parser('single', description='Generate a single sitemap by specifying parameters on the command line.', help='generate a single sitemap')
    parser_a.set_defaults(command='single')

    # ResourceSync server
    parser_a.add_argument('resourcesync-server-hostname', metavar='<resourcesync-server-hostname>', help='hostname of the ResourceSync source server')
    parser_a.add_argument('resourcesync-server-document-root', metavar='<resourcesync-server-document-root>', help='document root of the server (pass "apache" for "/var/www/html", or "tomcat" for "/var/lib/tomcat/webapps/default")')
    parser_a.add_argument('--resource-dir', metavar='<path>', help='path to a directory under the document root where <metadata-dir>s will be put for each collection (if unspecified, defaults to "/resourcesync")')
    parser_a.add_argument('--metadata-dir', metavar='<path>', help='path to a directory under the <resource-dir> where generated sitemaps will be put for the specified collection (if unspecified, defaults to <collection-name>)')

    # OAI-PMH data provider
    parser_a.add_argument('oai-pmh-base-url', metavar='<oai-pmh-base-url>', help='OAI-PMH base URL to which query parameters are appended')
    parser_a.add_argument('metadata-format', metavar='<oai-pmh-metadata-format>', choices=['oai_dc','mods'], help='"oai_dc" or "mods"')
    parser_a.add_argument('--no-set-param', action='store_const', const=True, help='the collection/set name is specified in the base URL, not as an OAI-PMH query parameter')

    parser_a.add_argument('strategy', metavar='<strategy>', choices=['resourcelist', 'new_changelist', 'inc_changelist'], help='"resourcelist", "new_changelist", or "inc_changelist"')
    parser_a.add_argument('collection-name', metavar='<collection-name>', help='name of the collection of resources to generate capability documents for')


    ### Subcommand - multiple sitemaps
    parser_b = subparsers.add_parser('multi', description='Generate multiple sitemaps by specifying parameters as rows in a CSV.', help='generate multiple sitemaps')
    parser_b.set_defaults(command='multi')
    parser_b.add_argument('config-file', metavar='<config-file>', help='path to config file containing information for each collection to process')


    args = vars(parser.parse_args())


    logging.config.fileConfig(os.path.abspath(os.path.expanduser(config['Logging']['config'])))

    logger = logging.getLogger('root')
    logger.info('--- STARTING RUN ---')
    logger.info('')
    logger.debug('Program arguments:')
    logger.debug('')

    out = dumps(args, indent=4)
    for line in out.splitlines():
        logger.debug(line)
    logger.debug('')


    collections = []

    if args['command'] == 'single':
        collection = {}
        collection['collection_name'] = args['collection-name']
        collection['resourcesync_url'] = args['resourcesync-server-hostname']
        collection['strategy'] = args['strategy']

        # some logic to set default values
        collection['document_root'] = '/var/www/html' if args['resourcesync-server-document-root'] == 'apache' else '/var/lib/tomcat/webapps/default' if args['resourcesync-server-document-root'] == 'tomcat' else args['resourcesync-server-document-root']
        collection['resource_dir'] = args['resource_dir'] if args['resource_dir'] is not None else 'resourcesync'
        collection['metadata_dir'] = args['metadata_dir'] if args['metadata_dir'] is not None else collection['collection_name']

        collection['oaipmh_base_url'] = args['oai-pmh-base-url']
        collection['oaipmh_set'] = args['collection-name'] if args['no_set_param'] is None else None
        collection['oaipmh_metadataprefix'] = args['metadata-format']

        collections.append(collection)

    elif args['command'] == 'multi':
        try:
            with open(args['config-file'], 'r') as f:
                csvreader = csv.DictReader(f, delimiter=',', quotechar='|')
                try:
                    for row in csvreader:
                        collection = {}
                        collection['collection_name'] = row['collection-name']
                        collection['resourcesync_url'] = row['resourcesync-server-hostname']
                        collection['strategy'] = row['strategy']

                        # some logic to set default values
                        collection['document_root'] = '/var/www/html' if row['resourcesync-server-document-root'] == 'apache' else '/var/lib/tomcat/webapps/default' if row['resourcesync-server-document-root'] == 'tomcat' else row['resourcesync-server-document-root']
                        collection['resource_dir'] = row['resource-dir'] if row['resource-dir'] is not '' else 'resourcesync'
                        collection['metadata_dir'] = row['metadata-dir'] if row['metadata-dir'] is not '' else collection['collection_name']

                        collection['oaipmh_base_url'] = row['oai-pmh-base-url']
                        collection['oaipmh_set'] = row['collection-name'] if row['no-set-param'] is '' else None
                        collection['oaipmh_metadataprefix'] = row['metadata-format']

                        collections.append(collection)
                except csv.Error as e:
                    logger.critical('File {}, line {}: {}'.format(args['config-file'], reader.line_num, e))
                    exit(1)
        except:
            logger.critical('Invalid command line argument: {} does not exist'.format(args['config-file']))
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
                                  metadata_dir=collection['metadata_dir'],
                                  description_dir=collection['document_root'],
                                  url_prefix='{}/{}'.format(collection['resourcesync_url'], collection['resource_dir']),
                                  is_saving_sitemaps=True)
                rs.execute()
            except Exception as e:
                logger.error('Unable to generate "{}" for collection "{}": {}'.format(
                    collection['strategy'],
                    collection['collection_name'],
                    e))

    logger.info('                              ')
    logger.info('---  ENDING RUN  ---')

if __name__ == '__main__':
    main()
