import time

class Heartbeat:
  TIMEOUT_LIMIT = 10
  server_heartbeats = {}

  def check_timeouts_against_interval(self, interval):
    print('Checking for expired heartbeats...')
    # Should lock hb dict
    timeouted_servers = []

    for server in self.server_heartbeats.keys():
      if time.time() - self.server_heartbeats[server] >= interval:
        timeouted_servers.append(server)

    # Should unlock hb dict
    return timeouted_servers

  def watch_timeouts(self, interval, callback):
    print('Starting to watch heartbeat timeouts...')
    while True:
      timeouted_servers = self.check_timeouts_against_interval(interval)
      
      if timeouted_servers:
        print('Some servers heartbeats timed out...')
        self.remove_heartbeats_from_dict(timeouted_servers)
        callback(timeouted_servers)
      
      time.sleep(30)

  def remove_heartbeats_from_dict(self, heartbeats_to_remove):
    print('Removing expired heartbeats from dictionary...')
    # Should lock hb dict
    for heartbeat in heartbeats_to_remove:
      self.server_heartbeats.pop(heartbeat, None)
    # Should unlock hb dict

  def receive_heartbeat(self, server_id):
    # Should lock hb dict
    print(f'Heartbeat received from {server_id}')
    self.server_heartbeats[server_id] = time.time()
    # Should unlock hb dict