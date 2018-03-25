
#!/usr/bin/env python3

from PyQt5 import QtWidgets as W, QtCore as C, QtGui as G
from .bus import Sub, Pull


class Background(C.QObject):
    def __init__(self, receiver):
        super().__init__()

        self.receiver = receiver
        self.running = True
        self.thread = None

    @classmethod
    def as_thread(cls, *args, **kwargs):
        thread = C.QThread()
        inst = cls(*args, **kwargs)
        inst.moveToThread(thread)

        if isinstance(inst.receiver, Sub):
            thread.started.connect(inst.sub_loop)
        elif isinstance(inst.receiver, Pull):
            thread.started.connect(inst.pull_loop)

        inst.thread = thread
        C.QTimer.singleShot(0, thread.start)

        return inst

    def quit_and_wait(self):
        if self.thread:
            self.thread.quit()
            self.thread.wait()

    def sub_loop(self):
        while self.running:
            topic, message = self.receiver.recv()
            method = getattr(self, 'on_' + topic, None)
            if callable(method):
                method(message)

    def pull_loop(self):
        while self.running:
            message = self.receiver.recv()
            self.on_message(message)