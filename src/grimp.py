#!/usr/bin/python3.4
## Pieterjan Montens 2016
## https://github.com/PieterjanMontens
import optparse
import json, yaml
import sys,os, os.path, importlib
#from grimp_importer import Importer
from solr_importer import SolrImporter
import logging, logging.config, logging.handlers

abspath = os.path.abspath(__file__)
os.chdir(os.path.dirname(abspath))

with open('./logging.conf','r') as f:
    logConf = yaml.load(f)
logging.config.dictConfig(logConf)
logger = logging.getLogger('grimplogger')

##################################################################### MAIN STUFF
################################################################################
def main():

    ############ Handle input & parameters
    p = optparse.OptionParser()
    p.add_option('--cfg','-c',action='store',default=None, help="Config to apply")
    p.add_option('--test','-t',action='store_true',default=False, help="Test import")
    p.add_option('--quiet','-q',action="store_true", default=False, help="Quiet mode")
    p.add_option('--no_out',action="store_true", default=False, help="Disable pipe mode")
    p.add_option('--debug',action="store_true", default=False, help="Enable debug mode")

    options, arguments = p.parse_args()
    logger.info('Grimp started')

    if options.debug:
        logger.setLevel(logging.DEBUG)

    if not options.cfg:
        logger.error('No config file specified')
        p.error('Config file not specified')

    confpath = options.cfg + '_cfg'
    logger.debug('Config file used:%s' % confpath)

    if not os.path.isfile(confpath + '.py'):
        logger.error('Config file does not exist')
        p.error('Config file {0} does not exist'.format(confpath))

    cfg = importlib.import_module(confpath)
    params = cfg.CFG

    if params['type'] == 'solr':
        logger.info('Solr importer selected')
        agent = SolrImporter(params)

    if options.test == True:
        logger.info('Testing enabled')
        agent.test_enable()

    ############ Read JSON Input
    for line in sys.stdin:
        try:
            json_input = json.loads(line)
            agent.imports(json_input)

            if not options.no_out:
                sys.stdout.write(line)

        except ValueError as e:
            if not options.quiet:
                logger.exception(e)
                logger.warning('Warning: line failed to parse, probably not valid json\n{0}'.format(line))

    ############ Job's done !
    exit(0)

########################################################################### INIT
################################################################################
if __name__ == '__main__':
  main()
