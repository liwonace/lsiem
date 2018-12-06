#!/usr/bin/python

import socket
import sys
from kafka import KafkaProducer
from kafka.errors import KafkaError

def print_help():
    print "Usage: listener.py <HOST> <PORT>"
    print "   HOST - ip address of local host"
    print "   PORT - local port number"

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print_help()
        exit(1)

    LHOST = sys.argv[1]
    LPORT = int( sys.argv[2] )

    sock = socket.socket( socket.AF_INET, socket.SOCK_DGRAM )
    sock.bind( (LHOST, LPORT) )

    producer = KafkaProducer(bootstrap_servers=['192.168.7.70:9092'])

    def on_send_success(record_metadata):
        print(record_metadata.topic)
        print(record_metadata.partition)
        print(record_metadata.offset)

    def on_send_error(excp):
        log.error('I am an errback', exc_info=excp)
        # handle exception
    
    while True:
        data, addr = sock.recvfrom(1024)
        sys.stdout.write( data )
        
        producer.send('android', key=b'foo', value=data)
        producer.send('android', data).add_callback(on_send_success).add_errback(on_send_error)
        producer.flush()
        sys.stdout.flush()
