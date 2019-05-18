# Broker MQTT in C++
Message Queueing Telemetry Transport (MQTT) implemented in C++.
Broker, Publisher and Subscriber classes.
The broker receive the messages send by publishers or subscribers. It uses multithreading to process the messages, and mutex to lock and avoid the simultaneous access.
