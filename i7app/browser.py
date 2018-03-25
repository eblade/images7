#!/usr/bin/env python3

from PyQt5 import QtWidgets as W, QtCore as C, QtGui as G


from images7 import date


class BrowserWidget(W.QTreeWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setHeaderHidden(True)

    def load(self):
        self.clear()

        by_date = W.QTreeWidgetItem(self)
        by_date.setText(0, 'By Date')

        query = date.DateQuery(reverse=True)
        feed = date.get_dates(query)

        for de in feed.entries:
            DateItem(by_date, de)


class DateItem(W.QTreeWidgetItem):
    def __init__(self, parent=None, date_entry=None):
        super().__init__(parent)
        self.date = date_entry
        self.setText(0, date_entry.date)

