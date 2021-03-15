# Corti Developer Test
# Author: Simon Aertssen
# Date: 12/03/2020

# Instructions: run with "python3 main.py"

import os
import socket
import hashlib
from threading import Thread, Event

from file_handlers import FileReader, FileWriter, QueuePoint


def read_and_write(send_line, recv_line, stop_event):
    print("Message start ----")
    while not stop_event.isSet():
        try:
            send_line(stop_event)
            recv_line(stop_event)
        except socket.timeout:
            # Then the reader has finished but the stop_event was not set
            pass
    print("Message end ----")


def check_messages_are_the_same(message_to_send, message_received):
    if not os.path.isfile(message_to_send) or not os.path.isfile(message_received):
        raise FileNotFoundError

    # Function-in-function to declare the scope
    def get_hash(path_to_file):
        BLOCK_SIZE = 65536
        hashed = hashlib.sha256()
        with open(path_to_file, 'rb') as f:
            while True:
                data = f.read(BLOCK_SIZE)
                if not data:
                    break
                    hashed.update(data)
        return hashed.hexdigest()

    # Compare each character, do not simply compare memory locations
    if get_hash(message_to_send) != get_hash(message_received):
        raise ValueError("Messages are not the same")
    else:
        print("Messages are the same! Good job!")


def execute_corti_developer_test():
    # Addresses: can also be a url
    HOST = socket.gethostname()
    IP = socket.gethostbyname(HOST)
    PORT = 8080
    print(f"{HOST} is running the corti developer test.")

    # Message file names
    message_file_to_send = "message_to_send.txt"
    message_file_receivd = "message_received.txt"

    # Objects were started in threads so no need for multiple files
    server = QueuePoint(IP, PORT)  # Actual server to which we send the text
    reader = FileReader(IP, PORT, message_file_to_send)  # Sending node as a 'client'
    writer = FileWriter(IP, PORT, message_file_receivd)  # Listening node as a 'server'

    # Start the thread that manages the reading and writing:
    read_and_write_thread = Thread(target=read_and_write,
                                   args=(reader.sendLines, writer.recvLine, server.stopCommunicating),
                                   name="File IO thread",
                                   daemon=False)
    read_and_write_thread.start()
    read_and_write_thread.join()

    # Check if sent and received message are the same.
    # This is performed easily by comparing the hash of each file.
    check_messages_are_the_same(message_file_to_send, message_file_receivd)


if __name__ == '__main__':
    execute_corti_developer_test()
