from http.server import BaseHTTPRequestHandler, HTTPServer
import configparser
import os
import re
import requests
import sys
import time
from urllib.error import HTTPError
from urllib.request import urlretrieve, url2pathname, Request, urlopen
from socketserver import ThreadingMixIn
from urllib.parse import urlparse
from email import message_from_bytes

def readConfigAndDefaults():
  myconfig = configparser.ConfigParser()
  myconfig.read('scup.cfg')
  return myconfig['DEFAULT']

def _isPathCached(cache_filename):
  while True:
    if os.path.exists('%s.lock' % cache_filename):
      time.sleep(5)
      continue
    return os.path.exists(cache_filename)

BYTE_RANGE_RE = re.compile(r'bytes=(\d+)-(\d+)?$')
def _parse_byte_range(byte_range):
  if byte_range.strip() == '':
    return None, None
  m = BYTE_RANGE_RE.match(byte_range)
  if not m:
    raise ValueError('Invalid byte range %s' % byte_range)
  first, last = [x and int(x) for x in m.groups()]
  if last and last < first:
    raise ValueError('Invalid byte range %s' % byte_range)
  return first, last

def send_proxy_pac(self):
  self.send_response(200)
  self.send_header('Content-type', 'application/x-ns-proxy-autoconfig')
  self.end_headers()
  self.wfile.write(bytes('''function FindProxyForURL(url, host) {
  if (shExpMatch(url, "http://%s/*"))
    return "PROXY %s:%s";
  return "DIRECT";
}''' % (config['remote_host'], config['proxy_ip'], config['proxy_port'], 'utf-8')))

def send_stats(self):
  self.send_response(200)
  self.send_header('Content-type', 'text/html')
  self.end_headers()
  self.wfile.write(bytes('TODO: this page', 'utf-8'))

class ThreadingSimpleServer(ThreadingMixIn, HTTPServer):
  pass

class CacheHandler(BaseHTTPRequestHandler):
  def do_GET(self):
    request_path = urlparse(self.path).path
    if request_path == '/proxy.pac':
      send_proxy_pac(self)
      return
    elif request_path == '/stats':
      send_stats(self)
      return
    success_code = 200
    start_byte = 0
    end_byte = 0
    if 'Range' in self.headers:
      success_code = 206
      try:
        start_byte, end_byte = _parse_byte_range(self.headers['Range'])
      except ValueError as e:
        self.send_error(400, 'Invalid byte range')
        return
    cache_filename = os.path.join(config['cache_path'], url2pathname(request_path)[1:])
    if not _isPathCached(request_path):
      print("Cache miss for %s" % (request_path))
      if not os.path.exists(os.path.dirname(cache_filename)):
        try:
          os.makedirs(os.path.dirname(cache_filename))
        except OSError as exc: # Guard against race condition
          if exc.errno != errno.EEXIST:
            raise
      self.send_response(success_code)
      remote_url = '%s://%s%s' % (config['remote_protocol'], config['remote_host'], request_path)
      r = requests.get(remote_url, stream=True)
      headers = r.headers
      for header, value in headers.items():
        if header in ['Date', 'Server']:
          continue
        self.send_header(keyword=header, value=value)
      self.end_headers()
      with open(cache_filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=config['chunk_size']):
          if chunk: # skip keep-alive
            f.write(chunk)
            try:
              self.wfile.write(chunk)
            except (ConnectionResetError, TimeoutError):
              print('client seems to have hungup. Maybe we\'ll catch them on the flip side...')
              return
      with open('%s.headers' % cache_filename, mode='wb') as h:
        for header, value in headers.items():
          h.write(bytes('%s: %s\n' % (header, value), 'utf-8'))
      return
    print("Cache hit from %s" % (cache_filename))
    headers_b = open('%s.headers' % cache_filename, 'rb').read()
    headers = message_from_bytes(headers_b)
    self.send_response(success_code)
    for header, value in headers.items():
      if header in ['Date', 'Server']:
        continue
      self.send_header(keyword=header, value=value)
    self.end_headers()
    with open(cache_filename, mode='rb') as f:
      f.seek(start_byte)
      while True:
        chunk = f.read(config['chunk_size'])
        if not chunk:
          break
        try:
          self.wfile.write(chunk)
        except (ConnectionResetError, TimeoutError):
          print('client seems to have hungup. Maybe we\'ll catch them on the flip side...')
          return

def run():
  global config
  config = readConfigAndDefaults()
  print('Cache path: %s' % config['cache_path'])
  server = ThreadingSimpleServer((config['proxy_ip'], int(config['proxy_port'])), CacheHandler)
  print('started proxy server at %s:%s' % (config['proxy_ip'], config['proxy_port']))
  try:
    while True:
      sys.stdout.flush()
      server.handle_request()
  except KeyboardInterrupt:
    print("Finished")

if __name__ == '__main__':
  run()