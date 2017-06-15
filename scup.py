from http.server import BaseHTTPRequestHandler, HTTPServer
import glob
import pprint
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

def _cleanupLockFiles():
  for f in glob.glob(pathname='%s/**/*.lock' % cache_path, recursive=True):
    actual_file = f[:-5]
    print('removing stale %s' % actual_file)
    try:
      os.remove(actual_file)
      os.remove('%s.headers' % actual_file)
    except IOError:
      pass
    os.remove(f)

def _isFileCached(cache_filename):
  while True:
    if os.path.exists('%s.lock' % cache_filename):
      time.sleep(5)
      continue
    return os.path.exists(cache_filename)

def _getPathSize(path):
  total_size = 0
  for dirpath, dirnames, filenames in os.walk(path):
    for f in filenames:
      fp = os.path.join(dirpath, f)
      total_size += os.path.getsize(fp)
  return total_size

def _makeFileFitInCache(path_only):
  max_cache_size = 1024*1024*1024*2 # 2gb
  cur_cache_size = int(_getPathSize(cache_path))
  req = Request('http://dl.google.com'+path_only, method='HEAD')
  resp = urlopen(req)
  new_file_size = int(resp.getheader('Content-Length', 1024*1024*1024))
  if new_file_size + cur_cache_size < max_cache_size:
    return

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

class ThreadingSimpleServer(ThreadingMixIn, HTTPServer):
  pass

class CacheHandler(BaseHTTPRequestHandler):
    def do_GET(self):
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
      path_only = urlparse(self.path).path
      if path_only == '/proxy.pac':
        self.send_response(200)
        self.send_header('Content-type', 'application/x-ns-proxy-autoconfig')
        self.end_headers()
        self.wfile.write(bytes('''function FindProxyForURL(url, host) {
  if (shExpMatch(url, "http://dl.google.com/*"))
    return "PROXY 192.168.1.4:8080";
  return "DIRECT";
}''', 'utf-8'))
        return
      cache_filename = os.path.join(cache_path, url2pathname(path_only)[1:])
      if not _isFileCached(cache_filename):
        print("Cache miss for %s" % (path_only))
        if not os.path.exists(os.path.dirname(cache_filename)):
          try:
            os.makedirs(os.path.dirname(cache_filename))
          except OSError as exc: # Guard against race condition
            if exc.errno != errno.EEXIST:
              raise
        open('%s.lock' % cache_filename, 'a').close()
        _makeFileFitInCache(path_only)
        self.send_response(success_code)
        r = requests.get("http://dl.google.com"+path_only, stream=True)
        headers = r.headers
        for header, value in headers.items():
          if header in ['Date', 'Server']:
            continue
          self.send_header(keyword=header, value=value)
        self.end_headers()
        with open(cache_filename, 'wb') as f:
          for chunk in r.iter_content(chunk_size=1024*1024): # 1mb chunk
            if chunk: # skip keep-alive
              f.write(chunk)
              try:
                self.wfile.write(chunk)
              except (ConnectionResetError, TimeoutError):
                print('client seems to have hungup. Maybe we\'ll catch them on the flip side...')
                os.remove(cache_filename)
                os.remove('%s.lock' % cache_filename)
                return
        with open('%s.headers' % cache_filename, mode='wb') as h:
          for header, value in headers.items():
            h.write(bytes('%s: %s\n' % (header, value)))
        os.remove('%s.lock' % cache_filename)
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
      print('start: %s\nend: %s' % (start_byte, end_byte))
      with open(cache_filename, mode='rb') as f:
        f.seek(start_byte)
        while True:
          chunk = f.read(1024*1024)
          if not chunk:
            break
          try:
            self.wfile.write(chunk)
          except (ConnectionResetError, TimeoutError):
            print('client seems to have hungup. Maybe we\'ll catch them on the flip side...')
            return

def run():
  global cache_path
  cache_path = os.path.join(os.path.dirname(os.path.realpath(__file__)) if not getattr(sys, u'frozen', False) else os.path.dirname(sys.executable), 'cache')
  print('Cache path: %s' % cache_path)
  _cleanupLockFiles()
  server = ThreadingSimpleServer(('', 8080), CacheHandler)
  try:
    while True:
      sys.stdout.flush()
      server.handle_request()
  except KeyboardInterrupt:
    print("Finished")

if __name__ == '__main__':
  run()