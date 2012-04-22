from twisted.internet import task

from scrapy.xlib.pydispatch import dispatcher
from scrapy.exceptions import NotConfigured
from scrapy.conf import settings
from scrapy import log, signals

class Slot(object):

    def __init__(self):
        self.items = 0
        self.itemsprev = 0
        self.pages = 0
        self.pagesprev = 0
        self.dupes = 0
        self.dupesprev = 0

class LogStats(object):
    """Log basic scraping stats periodically"""

    def __init__(self):
        self.interval = settings.getfloat('LOGSTATS_INTERVAL')
        if not self.interval:
            raise NotConfigured
        self.slots = {}
        self.multiplier = 60.0 / self.interval
        dispatcher.connect(self.item_scraped, signal=signals.item_scraped)
        dispatcher.connect(self.response_received, signal=signals.response_received)
        dispatcher.connect(self.spider_opened, signal=signals.spider_opened)
        dispatcher.connect(self.spider_closed, signal=signals.spider_closed)
        dispatcher.connect(self.engine_started, signal=signals.engine_started)
        dispatcher.connect(self.engine_stopped, signal=signals.engine_stopped)

    @classmethod
    def from_crawler(cls, crawler):
        o = cls()
        o.crawler = crawler
        return o

    def item_scraped(self, spider):
        self.slots[spider].items += 1

    def response_received(self, spider):
        self.slots[spider].pages += 1

    def spider_opened(self, spider):
        self.slots[spider] = Slot()

    def spider_closed(self, spider):
        del self.slots[spider]

    def engine_started(self):
        self.tsk = task.LoopingCall(self.log)
        self.tsk.start(self.interval)

    def log(self):
        engine = self.crawler.engine
        for spider, slot in self.slots.items():
            slot.dupes = engine.slots[spider].scheduler.dupescount
            irate = (slot.items - slot.itemsprev) * self.multiplier
            prate = (slot.pages - slot.pagesprev) * self.multiplier
            drate = (slot.dupes - slot.dupesprev) * self.multiplier
            slot.pagesprev = slot.pages
            slot.itemsprev = slot.items
            slot.dupesprev = slot.dupes
            msg = "Crawled %d pages (at %d pages/min), " \
                  "scraped %d items (at %d items/min), " \
                  "discarded %d requests (at %d req/min)" \
            % (slot.pages, prate, slot.items, irate, slot.dupes, drate)
            log.msg(msg, spider=spider)

    def engine_stopped(self):
        if self.tsk.running:
            self.tsk.stop()
