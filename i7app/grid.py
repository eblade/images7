#!/usr/bin/env python3

from PyQt5 import QtWidgets as W, QtCore as C, QtGui as G

from images7.entry import FilePurpose
from images7.files import get_file_by_reference


class ThumbView(W.QGraphicsView):
    def __init__(self, parent=None):
        super().__init__(parent)

    def populate(self, feed):
        self.scene = W.QGraphicsScene(self)

        cols_per_row = 4

        for n, entry in enumerate(feed.entries):
            thumb = ThumbItem(entry)
            self.scene.addItem(thumb)

        self.setScene(self.scene)


class ThumbItem(W.QGraphicsPixmapItem):
    def __init__(self, entry):
        super().__init__()
        self.entry = entry
        self.proxy_ref = entry.get_file_by_purpose(FilePurpose.proxy)

        if self.proxy_ref:
            self.file = get_file_by_reference(self.proxy_ref.reference)
            print(self.file.to_json())
        else:
            self.file = None
