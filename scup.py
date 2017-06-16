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
import sqlite3
from urllib.parse import urlparse
from email import message_from_bytes

CLIENT_DROPPED_EXCEPTIONS = (BrokenPipeError, ConnectionResetError, TimeoutError)

def readConfigAndDefaults():
  in_config = configparser.ConfigParser()
  in_config.read('scup.cfg')
  my_config = {}
  for key, value in in_config['DEFAULT'].items():
    if key in ['proxy_port', 'chunk_size']:
      value = int(value)
    my_config[key] = value
  return my_config

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

def getSqlConnAndCur():
  sqlconn = sqlite3.connect(sqldbfile,
    detect_types=sqlite3.PARSE_DECLTYPES)
  sqlconn.text_factory = str
  sqlcur = sqlconn.cursor()
  return (sqlconn, sqlcur)

def send_proxy_pac(self):
  self.send_response(200)
  self.send_header('Content-type', 'application/x-ns-proxy-autoconfig')
  self.end_headers()
  self.wfile.write(bytes('''function FindProxyForURL(url, host) {
  if (shExpMatch(url, "http://%s/chromeos/*"))
    return "PROXY %s:%s";
  return "DIRECT";
}''' % (config['remote_host'], config['proxy_ip'], config['proxy_port']), 'utf-8'))

def send_stats(self):
  self.send_response(200)
  self.send_header('Content-type', 'text/html')
  self.end_headers()
  self.wfile.write(bytes('TODO: this page', 'utf-8'))

def initializeDB(sqlconn, sqlcur):
  print('Initializing %s' % sqldbfile)
  sqlcur.executescript('''
    CREATE TABLE files(remote_path TEXT UNIQUE,
                         local_file TEXT,
                         etag TEXT,
                         local_status TEXT,
                         first_seen TIMESTAMP,
                         last_seen TIMESTAMP,
                         content_length INTEGER,
                         downloaded_length INTEGER,
                         content_type TEXT);
    CREATE TABLE downloads(client_ip TEXT,
                           file_id TEXT,
                           bytes_read INTEGER,
                           start_time TIMESTAMP,
                           end_time TIMESTAMP);
  ''')
  sqlconn.commit()

def getPathCacheStatus(path, cache_filename, sqlconn, sqlcur):
  status = 'NOTCACHED'
  try:
    sqlcur.execute('''
         SELECT local_status, content_length FROM files
           where remote_path = ?''', ((path),))
  except sqlite3.OperationalError as e:
    print("SQL error:%s" % e)
    sys.exit(8)
  sqlresults = sqlcur.fetchall()
  for x in sqlresults:
    local_status = x[0]
    content_length = x[1]
    if os.path.exists(cache_filename):
      if local_status == 'PARTIAL':
        status = 'PARTIAL'
      elif local_status == 'CACHED' and os.path.getsize(cache_filename) == content_length:
        status = 'CACHED'
  return status

class ThreadingSimpleServer(ThreadingMixIn, HTTPServer):
  pass

class CacheHandler(BaseHTTPRequestHandler):
  def do_GET(self):
    request_path = urlparse(self.path).path
    if request_path == '/proxy.pac':
      send_proxy_pac(self)
      return
    sqlconn, sqlcur = getSqlConnAndCur()
    if request_path == '/stats':
      send_stats(self, sqlconn, sqlcur)
      return
    success_code = 200
    start_byte = 0
    bytes_sent_to_client = 0
    bytes_from_server = 0
    if 'Range' in self.headers:
      success_code = 206
      try:
        start_byte, _ = _parse_byte_range(self.headers['Range'])
      except ValueError as e:
        self.send_error(400, 'Invalid byte range')
        return
    cache_filename = os.path.join(config['cache_path'], url2pathname(request_path)[1:])
    cache_status = getPathCacheStatus(request_path, cache_filename, sqlconn, sqlcur)
    if cache_status == 'NOTCACHED':
      remote_url = '%s://%s%s' % (config['remote_protocol'], config['remote_host'], request_path)
      h = requests.head(remote_url)
      if h.status_code < 200 or h.status_code > 299:
        print('%s - %s: %s' % (self.client_address[0], h.status_code, h.reason))
        self.send_error(h.status_code, h.reason)
        return
      print("Cache miss for %s" % (request_path))
      if not os.path.exists(os.path.dirname(cache_filename)):
        try:
          os.makedirs(os.path.dirname(cache_filename))
        except OSError as exc: # Guard against race condition
          if exc.errno != errno.EEXIST:
            raise
      self.send_response(success_code)
      r = requests.get(remote_url, stream=True)
      headers = r.headers
      for header, value in headers.items():
        if header in ['Date', 'Server']:
          continue
        self.send_header(keyword=header, value=value)
      self.end_headers()
      sqlconn, sqlcur = getSqlConnAndCur()
      etag = headers.get('Etag', '')
      content_length = headers.get('Content-length', 0)
      content_type = headers.get('Content-type', 'application/octet-stream')
      sqlcur.execute('''INSERT into files (remote_path, etag, local_status, first_seen, last_seen, content_length, content_type) VALUES
        (?, ?, 'PARTIAL', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?, ?)''', (request_path, etag, content_length, content_type))
      sqlconn.commit()
      send_to_client = True
      with open(cache_filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=config['chunk_size']):
          if chunk: # skip keep-alive
            bytes_from_server += len(chunk)
            f.write(chunk)
            if send_to_client:
              try:
                self.wfile.write(chunk)
              except CLIENT_DROPPED_EXCEPTIONS:
                print('client seems to have hungup. Still trying to finish file download')
                send_to_client = False
              bytes_sent_to_client += len(chunk)
      sqlcur.execute('''UPDATE files SET local_status = 'CACHED', last_seen = CURRENT_TIMESTAMP where remote_path = ?''', (request_path,))
      sqlconn.commit()
      return
    elif cache_status == 'PARTIAL':
      print("Partial cache hit for %s" % (request_path))
      self.send_response(success_code)
      self.end_headers()
      next_chunk_start = start_byte
      sqlcur.execute('''SELECT content_length FROM files WHERE remote_path = ?''', (request_path,))[0][0]
      full_file_size = sqlcur.fetchall()[0][0]
      while True:
        current_size = os.path.getsize(cache_filename)
        with open(cache_filename, mode='rb') as f:
          if current_size >= next_chunk_start + config['chunk_size']:
            while True:
              f.seek(next_chunk_start)
              chunk = f.read(config['chunk_size'])
              if not chunk:
                break
              try:
                self.wfile.write(chunk)
              except CLIENT_DROPPED_EXCEPTIONS:
                print('client seems to have hungup. Maybe we\'ll catch them on the flip side...')
                return
              bytes_sent_to_client += len(chunk)
              next_chunk_start += config['chunk_size']
          else:
            sleep(1)
    else:
      print("Cache hit from %s" % (cache_filename))
      self.send_response(success_code)
      self.end_headers()
      with open(cache_filename, mode='rb') as f:
        f.seek(start_byte)
        while True:
          chunk = f.read(config['chunk_size'])
          if not chunk:
            break
          try:
            self.wfile.write(chunk)
          except CLIENT_DROPPED_EXCEPTIONS:
            print('client seems to have hungup. Maybe we\'ll catch them on the flip side...')
            return
          bytes_sent_to_client += len(chunk)

def run():
  global config, sqldbfile
  config = readConfigAndDefaults()
  if not os.path.exists(config['cache_path']):
    os.mkdir(config['cache_path'])
  print('Cache path: %s' % config['cache_path'])
  sqldbfile = os.path.join(config['cache_path'], 'cache.sqlite')
  if not os.path.isfile(sqldbfile):
    sqlconn, sqlcur = getSqlConnAndCur()
    initializeDB(sqlconn, sqlcur)
  try:
    server = ThreadingSimpleServer((config['proxy_ip'], config['proxy_port']), CacheHandler)
  except OSError as e:
    print('Error: %s' % e)
    return
  print('started proxy server at %s:%s' % (config['proxy_ip'], config['proxy_port']))
  try:
    while True:
      sys.stdout.flush()
      server.handle_request()
  except KeyboardInterrupt:
    print("Finished")

if __name__ == '__main__':
  run()
