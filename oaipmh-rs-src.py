#!/usr/bin/python3.6
#
# oaipmh-rs-src.py

from resourcesync.resourcesync import ResourceSync, Parameters
from resourcesync.generators.oaipmh_generator import OAIPMHGenerator
import argparse

parser = argparse.ArgumentParser(description='Generate sitemaps for a ResourceSync source server.')

# ResourceSync server
parser.add_argument('resourcesync-server-hostname', metavar='<resourcesync-server-hostname>', nargs=1, help='hostname of the ResourceSync source server')
parser.add_argument('resourcesync-server-document-root', metavar='<resourcesync-server-document-root>', help='document root of the server (pass "apache" for "/var/www/html", or "tomcat" for "/var/lib/tomcat/webapps/default")')
parser.add_argument('--resource-dir', metavar='<path>', help='path to a directory under the document root where <metadata-dir>s will be put for each collection (if unspecified, defaults to "/resourcesync")')
parser.add_argument('--metadata-dir', metavar='<path>', help='path to a directory under the >resource-dir> where generated sitemaps will be put for the specified collection (if unspecified, defaults to <collection-name>')

# OAI-PMH data provider
parser.add_argument('oai-pmh-base-url', metavar='<oai-pmh-base-url>', nargs=1, help='OAI-PMH base URL to which query parameters are appended')
parser.add_argument('metadata-format', metavar='<oai-pmh-metadata-format>', nargs=1, choices=['oai_dc','mods'], help='"oai_dc" or "mods"')
parser.add_argument('--no-set-param', action='store_const', const=True, help='indicate that the collection/set name is specified in the base URL, not as an OAI-PMH query parameter')

parser.add_argument('strategy', metavar='<strategy>', nargs=1, choices=['resourcelist', 'new_changelist', 'inc_changelist'], help='"resourcelist", "new_changelist", or "inc_changelist"')
parser.add_argument('collection-name', metavar='<collection-name>', nargs=1, help='name of the collection of resources to generate capability documents for')

args = vars(parser.parse_args())

# some logic to set default values
document_root = '/var/www/html' if args['resourcesync-server-document-root'] == 'apache' else '/var/lib/tomcat/webapps/default' if args['resourcesync-server-document-root'] == 'tomcat' else args['resourcesync-server-document-root']
resource_dir = args['resource_dir'][0] if args['resource_dir'] is not None else 'resourcesync'
collection_name = args['collection-name'][0]
resourcesync_url = args['resourcesync-server-hostname'][0]

oaipmh_base_url = args['oai-pmh-base-url'][0]
oaipmh_set = args['collection-name'][0] if args['no_set_param'] is None else None
oaipmh_metadataprefix = args['metadata-format'][0]

my_generator = OAIPMHGenerator(params={
    'oaipmh_base_url':       oaipmh_base_url,
    'oaipmh_set':            oaipmh_set,
    'oaipmh_metadataprefix': oaipmh_metadataprefix})

rs = ResourceSync(generator=my_generator,
                  strategy=args['strategy'][0],
                  resource_dir='{}/{}'.format(document_root, resource_dir),
                  metadata_dir=collection_name,
                  description_dir=document_root,
                  url_prefix='{}/{}'.format(resourcesync_url, resource_dir),
                  is_saving_sitemaps=True)
rs.execute()
################################################################################
