from time import time
from urlparse import urlparse, urlunparse, urldefrag

from twisted.web.client import HTTPClientFactory
from twisted.web.http import HTTPClient
from twisted.internet import defer

from scrapy.http import Headers
from scrapy.utils.httpobj import urlparse_cached
from scrapy.responsetypes import responsetypes
from scrapy import optional_features

from scrapy import log


def _parsed_url_args(parsed):
    path = urlunparse(('', '', parsed.path or '/', parsed.params, parsed.query, ''))
    host = parsed.hostname
    port = parsed.port
    scheme = parsed.scheme
    netloc = parsed.netloc
    if port is None:
        port = 443 if scheme == 'https' else 80
    return scheme, netloc, host, port, path


def _parse(url):
    url = url.strip()
    parsed = urlparse(url)
    return _parsed_url_args(parsed)


class ScrapyHTTPPageGetter(HTTPClient):

    delimiter = '\n'
    tunnel_started = False
    tunnel_headers = ['Host', 'Proxy-Connection', 'Proxy-Authorization', 'User-Agent']

    def connectionMade(self):
        # bucket for response headers
        self.headers = Headers()
        if self.factory.use_tunnel:
            hp = "%s:%s" % (self.factory.tunnel_to_host,
                            self.factory.tunnel_to_port)
            log.msg("Sending CONNECT %s" % hp, log.DEBUG)
            self._send_command('CONNECT', hp)
            self._send_tunnel_headers()
        else:
            self._send_everything()

    def _send_command(self, command, path):
        if self.factory.use_tunnel and not self.tunnel_started:
            http_version = "1.1"
        else:
            http_version = "1.0"
        cmdline = '%s %s HTTP/%s\r\n' % (command, path, http_version)
        log.msg('Sending command: %r' % cmdline)
        self.transport.write(cmdline)

    def _send_everything(self):
        self._send_method()
        self._send_headers()
        self._send_body()

    def _send_method(self):
        self._send_command(self.factory.method, self.factory.path)

    def _send_single_header(self, key):
        for value in self.factory.headers.getlist(key):
            log.msg('Sending header %s: %s' % (key, value))
            self.sendHeader(key, value)

    def _send_tunnel_headers(self):
        for key in self.tunnel_headers:
            self._send_single_header(key)
        self.endHeaders()

    def _send_headers(self):
        for key in self.factory.headers.keys():
            # Skip proxy headers in case of tunneling with CONNECT method
            if self.factory.use_tunnel and key.startswith('Proxy-'):
                continue
            self._send_single_header(key)
        self.endHeaders()

    def _send_body(self):
        if self.factory.body is not None:
            self.transport.write(self.factory.body)

    def lineReceived(self, line):
        if self.factory.use_tunnel and not self.tunnel_started:
            log.msg("LINE: %s" % line)

        if self.factory.use_tunnel and not self.tunnel_started and not line.rstrip():
            # End of headers from the proxy in response to our CONNECT request
            # Skip the call to HTTPClient.lienReceived for now, since otherwise
            # it would switch to row mode.
            self._start_tunnel()
        else:
            return HTTPClient.lineReceived(self, line.rstrip())

    def _start_tunnel(self):
        log.msg("starting Tunnel")
        # We'll get a new batch of headers through the tunnel. This sets us
        # up to capture them.
        self.firstLine = True
        self.tunnel_started = True
        # Switch to SSL
        ctx = ClientContextFactory()
        self.transport.startTLS(ctx, self.factory)
        # And send the normal request:
        self._send_everything()

    def handleHeader(self, key, value):
        if self.factory.use_tunnel and not self.tunnel_started:
            log.msg('Proxy header "%s: %s"' % (key, value))
            pass  # maybe log headers for CONNECT request?
        else:
            self.headers.appendlist(key, value)

    def handleStatus(self, version, status, message):
        if self.factory.use_tunnel and not self.tunnel_started:
            self.tunnel_status = status
        else:
            self.factory.gotStatus(version, status, message)

    def handleEndHeaders(self):
        self.factory.gotHeaders(self.headers)

    def connectionLost(self, reason):
        HTTPClient.connectionLost(self, reason)
        self.factory.noPage(reason)

    def handleResponse(self, response):
        if self.factory.method.upper() == 'HEAD':
            self.factory.page('')
        else:
            self.factory.page(response)
        self.transport.loseConnection()

    def timeout(self):
        self.transport.loseConnection()
        msg = "Getting %s took longer than %s seconds." % (self.factory.url,
                                                           self.factory.timeout)
        self.factory.noPage(defer.TimeoutError(msg))


class ScrapyHTTPClientFactory(HTTPClientFactory):
    """Scrapy implementation of the HTTPClientFactory overwriting the
    serUrl method to make use of our Url object that cache the parse
    result.
    """

    protocol = ScrapyHTTPPageGetter
    waiting = 1
    noisy = False
    followRedirect = False
    afterFoundGet = False

    def __init__(self, request, timeout=180):
        self.url = urldefrag(request.url)[0]
        self.method = request.method
        self.body = request.body or None
        self.headers = Headers(request.headers)
        self.response_headers = None
        self.timeout = request.meta.get('download_timeout') or timeout
        self.start_time = time()
        self.deferred = defer.Deferred().addCallback(self._build_response, request)

        # Fixes Twisted 11.1.0+ support as HTTPClientFactory is expected
        # to have _disconnectedDeferred. See Twisted r32329.
        # As Scrapy implements it's own logic to handle redirects is not
        # needed to add the callback _waitForDisconnect.
        # Specifically this avoids the AttributeError exception when
        # clientConnectionFailed method is called.
        self._disconnectedDeferred = defer.Deferred()

        self._set_connection_attributes(request)

        # set Host header based on url
        self.headers.setdefault('Host', self.netloc)

        # set Content-Length based len of body
        if self.body is not None:
            self.headers['Content-Length'] = len(self.body)
            # just in case a broken http/1.1 decides to keep connection alive
            self.headers.setdefault("Connection", "close")

    def _build_response(self, body, request):
        request.meta['download_latency'] = self.headers_time - self.start_time
        status = int(self.status)
        headers = Headers(self.response_headers)
        respcls = responsetypes.from_args(headers=headers, url=self.url)
        return respcls(url=self.url, status=status, headers=headers, body=body)

    def _set_connection_attributes(self, request):
        parsed = urlparse_cached(request)
        self.scheme, self.netloc, self.host, self.port, self.path = _parsed_url_args(parsed)
        self.use_tunnel = False
        proxy = request.meta.get('proxy')
        if proxy:
            old_scheme, old_host, old_port = self.scheme, self.host, self.port
            self.scheme, _, self.host, self.port, _ = _parse(proxy)
            if old_scheme == "https":
                self.use_tunnel = True
                self.tunnel_to_host = old_host
                self.tunnel_to_port = old_port
                self.headers['Proxy-Connection'] = 'keep-alive'
            else:
                self.path = self.url

    def gotHeaders(self, headers):
        self.headers_time = time()
        self.response_headers = headers


if 'ssl' in optional_features:
    from twisted.internet.ssl import ClientContextFactory
    from OpenSSL import SSL
else:
    ClientContextFactory = object


class ScrapyClientContextFactory(ClientContextFactory):
    "A SSL context factory which is more permissive against SSL bugs."
    # see https://github.com/scrapy/scrapy/issues/82
    # and https://github.com/scrapy/scrapy/issues/26

    def __init__(self):
        # see this issue on why we use TLSv1_METHOD by default
        # https://github.com/scrapy/scrapy/issues/194
        self.method = SSL.TLSv1_METHOD

    def getContext(self):
        ctx = ClientContextFactory.getContext(self)
        # Enable all workarounds to SSL bugs as documented by
        # http://www.openssl.org/docs/ssl/SSL_CTX_set_options.html
        ctx.set_options(SSL.OP_ALL)
        return ctx
