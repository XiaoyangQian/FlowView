# Referenced from IDEA Thrive (https://github.intuit.com/idea/thrive)

from flowview.load_handler import LoadHandler
from flowview.setup_handler import SetupHandler
from flowview.cleanup_handler import CleanupHandler
from optparse import OptionParser
import logging
import log


parser = OptionParser()

parser.add_option("--config-file", dest="config_file", action="store")

parser.add_option("--phase",dest="phase",action="store")


(options,args) = parser.parse_args()

log.init_logging()
logger = logging.getLogger(__name__)

try:
    if options.phase == "setup":
        handler = SetupHandler(options.config_file)
    elif options.phase == "load":
        handler = LoadHandler(options.config_file)
    elif options.phase == "cleanup":
        handler = CleanupHandler(options.config_file)
    else:
        handler = None
        logger.error("Illegal option phase: %s" % options.phase)
    handler.execute()

except NameError:
    print "Illegal option topic: %s" % options.topic
