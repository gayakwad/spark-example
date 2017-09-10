1. Session Windows
===============

Sessions are windows that capture some period of activity over a subset of the data, in this case per user_id. Typically they are defined by a timeout gap. Any events that occur within a span of time less than the timeout are grouped together as a session.

Given a stream of user events, and a timeout gap of 30 minutes, calculate the messages sent per session, the top channel_id, and the number of messages sent to that channel in the session

Example

Input

{'timestamp':'2017-02-07T00:00:00Z', 'user_id': 1, 'event': 'send_message', 'channel_id': 1, 'message_id': 1}
{'timestamp':'2017-02-07T00:00:20Z', 'user_id': 2, 'event': 'send_message', 'channel_id': 1, 'message_id': 2}
{'timestamp':'2017-02-07T00:00:30Z', 'user_id': 2, 'event': 'send_message', 'channel_id': 1, 'message_id': 3}

Output

{'user_id': 1, 'session_start_ts': ..., 'session_end_ts': ..., 'messages_sent': ..., 'top_channel_id': ..., 'top_channel_messages_sent': ...}
