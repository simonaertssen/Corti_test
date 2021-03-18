import os
import sys
import time
import socket

from threading import Thread, Event


class MySocket(socket.socket):
    """
    Class used to extend the python socket with custom methods and attributes.
    """
    BUFFER_SIZE = 512  # Only a small buffer for each line
    def __init__(self, ip, port):
        super(MySocket, self).__init__(socket.AF_INET, socket.SOCK_STREAM)
        self.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.settimeout(1.0)  # Timeout after one second
        self.Address = (ip, port)

        startup_thread = Thread(target=self.connectSafely,
                                args=(),
                                name='Startup',
                                daemon=True)
        startup_thread.start()

    def __str__(self):
        """
        String representation. It's safe to mention class of inherited instance.
        """
        return self.__class__.__name__

    def renewSocket(self):
        super(Reader, self).__init__(socket.AF_INET, socket.SOCK_STREAM)

    def connect_or_bind(self):
        raise NotImplementedError

    def connectSafely(self, verbose=True):
        connected = False
        try:
            self.connect_or_bind()
            connected = True
            if verbose:
                print(f"{self} is safely connected")
        except socket.timeout:
            print(f"{self} connection timed out")
        except ConnectionRefusedError as e:
            print(f"{self} connection refused: error code {e.errno}")

        if not connected:
            self.shutdownSafely(verbose=verbose)

    def releaseDependencies(self):
        """
        Release memory occupied by children of this class.
        """
        pass

    def shutdownSafely(self, verbose=True):
        if verbose:
            print(f"{self} shutting down safely")
        try:
            self.releaseDependencies()
            self.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.close()


class FileReader(MySocket):
    """
    Reads a file from disk and sends it line by line to the queue point.
    """
    EndOfFileEvent = Event()
    def __init__(self, ip, port, send_file_name):
        super(FileReader, self).__init__(ip, port)
        if not os.path.isfile(send_file_name):
            raise OSError(f"{send_file_name} is not a file")

        # Open the file once: much less expensive. All lines will be bytes objects
        self.file = open(send_file_name, 'rb')

    def connect_or_bind(self):
        self.connect(self.Address)

    def sendLines(self, stop_event):
        """
        For smaller files we can send all lines at once.
        """
        if stop_event.isSet():
            return
        lines = b''.join(self.file.readlines())
        if not lines:
            stop_event.set()
        self.send(lines)

    def sendLine(self, stop_event):
        """
        Send line by line.
        """
        if stop_event.isSet():
            return
        line = self.file.readline()
        if not line:
            stop_event.set()
        else:
            self.send(line)

    def releaseDependencies(self):
        self.file.close()


class FileWriter(MySocket):
    """
    Gets text line by line from the queue point and saves it to disk.
    """
    def __init__(self, ip, port, receive_file_name):
        super(FileWriter, self).__init__(ip, port)
        # Delete the received file if it exists:
        if os.path.isfile(receive_file_name):
            os.remove(receive_file_name)

        # Open the file once: much less expensive
        self.file = open(receive_file_name, 'a')

    def connect_or_bind(self):
        self.connect(self.Address)

    def recvLine(self, stop_event):
        if stop_event.isSet():
            return
        line = self.recv(self.BUFFER_SIZE).decode("utf-8")
        print(line[0:-1])
        self.file.write(line)

    def releaseDependencies(self):
        self.file.close()


class QueuePoint(MySocket):
    """
    Main queue service, which passes on a message.
    """
    def __init__(self, ip, port):
        super(QueuePoint, self).__init__(ip, port)
        self.stopCommunicating = Event()
        communication_thread = Thread(target=self.communicate,
                                      args=(self.stopCommunicating,),
                                      name="Communication Thread",
                                      daemon=True)
        communication_thread.start()

    def connect_or_bind(self):
        self.bind(self.Address)
        self.listen()

    def communicate(self, stop_event):
        # Accept connection from the file reader and read data
        client_recv, address = self.accept()

        # Accept connection from the file writer and send data
        client_send, address = self.accept()

        MAX_TIME = 20.0
        start_time = time.time()
        while time.time() - start_time < MAX_TIME:
            if stop_event.isSet():
                break
            data = client_recv.recv(self.BUFFER_SIZE)
            client_send.send(data)
