from threading import Lock
import time

class Heartbeat:
  server_heartbeats = {}
  heartbeats_lock = Lock()

  def check_timeouts_against_interval(self, interval):
    print('Checking for expired heartbeats...')
    timeouted_servers = []

    self.heartbeats_lock.acquire()
    for server in self.server_heartbeats.keys():
      if time.time() - self.server_heartbeats[server] >= interval:
        timeouted_servers.append(server)

    self.heartbeats_lock.release()
    return timeouted_servers

  def watch_timeouts(self, interval, callback):
    print('Starting to watch heartbeat timeouts...')
    while True:
      timeouted_servers = self.check_timeouts_against_interval(interval)
      
      if timeouted_servers:
        print('Some servers heartbeats timed out...')
        self.remove_heartbeats_from_dict(timeouted_servers)
        callback(timeouted_servers)
      
      time.sleep(interval)

  def remove_heartbeats_from_dict(self, heartbeats_to_remove):
    print('Removing expired heartbeats from dictionary...')
    self.heartbeats_lock.acquire()
    for heartbeat in heartbeats_to_remove:
      self.server_heartbeats.pop(heartbeat, None)
    self.heartbeats_lock.release()

  def receive_heartbeat(self, server_id):
    self.heartbeats_lock.acquire()
    print(f'Heartbeat received from {server_id}')
    self.server_heartbeats[server_id] = time.time()
    self.heartbeats_lock.release()
