#!/usr/bin/python3
#
# rs_oaipmh_src.py

#import json
from resourcesync.resourcesync import ResourceSync, Parameters
from resourcesync.generators.oaipmh_generator import OAIPMHGenerator
import csv
import argparse

def main():
    parser = argparse.ArgumentParser(description='Generate sitemaps for a ResourceSync source server.')
    subparsers = parser.add_subparsers(title='commands', metavar='COMMAND', description='Each command specifies a different mode for generating sitemaps. For detailed usage instructions, run `python3 rs_oaipmh_src.py COMMAND -h`.')


    ### Subcommand - single sitemap
    parser_a = subparsers.add_parser('single', description='Generate a single sitemap by specifying parameters on the command line.', help='generate a single sitemap')
    parser_a.set_defaults(command='single')

    # ResourceSync server
    parser_a.add_argument('resourcesync-server-hostname', metavar='<resourcesync-server-hostname>', nargs=1, help='hostname of the ResourceSync source server')
    parser_a.add_argument('resourcesync-server-document-root', metavar='<resourcesync-server-document-root>', help='document root of the server (pass "apache" for "/var/www/html", or "tomcat" for "/var/lib/tomcat/webapps/default")')
    parser_a.add_argument('--resource-dir', metavar='<path>', help='path to a directory under the document root where <metadata-dir>s will be put for each collection (if unspecified, defaults to "/resourcesync")')
    parser_a.add_argument('--metadata-dir', metavar='<path>', help='path to a directory under the >resource-dir> where generated sitemaps will be put for the specified collection (if unspecified, defaults to <collection-name>')

    # OAI-PMH data provider
    parser_a.add_argument('oai-pmh-base-url', metavar='<oai-pmh-base-url>', nargs=1, help='OAI-PMH base URL to which query parameters are appended')
    parser_a.add_argument('metadata-format', metavar='<oai-pmh-metadata-format>', nargs=1, choices=['oai_dc','mods'], help='"oai_dc" or "mods"')
    parser_a.add_argument('--no-set-param', action='store_const', const=True, help='indicate that the collection/set name is specified in the base URL, not as an OAI-PMH query parameter')

    parser_a.add_argument('strategy', metavar='<strategy>', nargs=1, choices=['resourcelist', 'new_changelist', 'inc_changelist'], help='"resourcelist", "new_changelist", or "inc_changelist"')
    parser_a.add_argument('collection-name', metavar='<collection-name>', nargs=1, help='name of the collection of resources to generate capability documents for')


    ### Subcommand - multiple sitemaps
    parser_b = subparsers.add_parser('multi', description='Generate multiple sitemaps by specifying parameters as rows in a CSV.', help='generate multiple sitemaps')
    parser_b.set_defaults(command='multiple')
    parser_b.add_argument('config-file', metavar='<config-file>', nargs=1, help='path to config file containing information for each collection to process')


    args = vars(parser.parse_args())
    #print(json.dumps(args, indent=4))


    collections = []

    if args['command'] == 'single':
        collection = {}
        collection['collection_name'] = args['collection-name'][0]
        collection['resourcesync_url'] = args['resourcesync-server-hostname'][0]
        collection['strategy'] = args['strategy'][0]

        # some logic to set default values
        collection['document_root'] = '/var/www/html' if args['resourcesync-server-document-root'] == 'apache' else '/var/lib/tomcat/webapps/default' if args['resourcesync-server-document-root'] == 'tomcat' else args['resourcesync-server-document-root']
        collection['resource_dir'] = args['resource_dir'][0] if args['resource_dir'] is not None else 'resourcesync'
        collection['metadata_dir'] = args['metadata_dir'][0] if args['metadata_dir'] is not None else collection['collection_name']

        collection['oaipmh_base_url'] = args['oai-pmh-base-url'][0]
        collection['oaipmh_set'] = args['collection-name'][0] if args['no_set_param'] is None else None
        collection['oaipmh_metadataprefix'] = args['metadata-format'][0]

        collections.append(collection)

    elif args['command'] == 'multiple':
        with open(args['config-file'][0]) as f:
            csvreader = csv.DictReader(f, delimiter=',', quotechar='|')
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

    #print(json.dumps(collections, indent=4))

    for collection in collections:
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

if __name__ == '__main__':
    main()
