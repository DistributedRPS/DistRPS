MESSAGE_CODES = {
  'ADD_TOPIC': '0',
  'DELETE_TOPIC': '1',
  'SEND_TOPIC_LIST': '2',
}

#   0-tournament starts
#   1-need input(R/P/S)
#   2-game state update
#   3-tournament ends
EVENT_TYPES = {
  'TOURNAMENT_START': '0',
  'REQUEST_INPUT': '1',
  'STATE_UPDATE': '2',
  'TOURNAMENT_END': '3',
}

#   0-want to join
#   1-player input
REQUEST_TYPES = {
  'WANT_TO_JOIN': '0',
  'PLAYER_INPUT': '1',
}