#!/usr/bin/env python3

import sys
import os
import zmq
import time
import logging

from PyQt5 import QtWidgets as W, QtCore as C, QtGui as G
from qtzevents.bus import Pub, Push
from qtzevents.background import Background

from images7.config import Config
from images7.system import System

from .grid import ThumbView
from .browser import BrowserWidget, DateItem


from images7 import (
    date,
    entry,
    files,
    importer,
    job,
)

from images7.job import (
    register,
    transcode,
    to_cut,
    to_main,
    calculate_hash,
    read_metadata,
    create_proxy,
    clean_cut,
)

from images7.analyse import exif
from images7.job.transcode import imageproxy


# Logging
FORMAT = '%(asctime)s [%(threadName)s] %(filename)s +%(levelno)s ' + \
            '%(funcName)s %(levelname)s %(message)s'
logging.basicConfig(
    format=FORMAT,
    level=logging.DEBUG if '-g' in sys.argv else logging.INFO,
    filename='log',
    filemode='w',
)


class View(W.QMainWindow):
    def __init__(self):
        super().__init__()

        self.context = zmq.Context(1)

        self.command = Push(self.context, 'command')
        self.system = None
        self.control = Control(self.system)

        self.setWindowTitle('Images7')
        self.setup_menu()
        self.setup_layout()
        #self.setup_model_event_handler()
        self.setup_control_event_handler()

        self.on_open('images.ini')
        logging.getLogger().setLevel(logging.DEBUG)

        self.show()

    def setup_menu(self):
        import_action = W.QAction('&Import', self)
        import_action.setShortcut('Ctrl+I')
        import_action.setStatusTip('Triggers an import of files from known cards')
        import_action.triggered.connect(self.on_import)

        reload_action = W.QAction('&Reload', self)
        reload_action.setShortcut('Ctrl+R')
        reload_action.setStatusTip('Reload the data browser')
        reload_action.triggered.connect(self.on_reload)

        m = self.menuBar()
        file_menu = m.addMenu('&File')
        file_menu.addAction(import_action)
        file_menu.addAction(reload_action)

    def setup_layout(self):
        self.setGeometry(100, 100, 1000, 800)

        splitter  = W.QSplitter(C.Qt.Horizontal)
        self.tree = BrowserWidget()
        self.tree.currentItemChanged.connect(self.on_browser_selection_changed)

        self.main = W.QStackedWidget()
        empty = W.QFrame()
        self.main.addWidget(empty)
        self.main.setCurrentWidget(empty)

        splitter.addWidget(self.tree)
        splitter.addWidget(self.main)
        splitter.setSizes([300, 700])

        self.setCentralWidget(splitter)

    def setup_model_event_handler(self):
        self.model_event_handler = ModelEventHandler.as_thread(
            self.system.event.subscriber('state', 'system', 'error'))
        self.model_event_handler.message.connect(self.on_message)
        self.model_event_handler.error.connect(self.on_error)

    def setup_control_event_handler(self):
        self.control_event_handler = ControlEventHandler.as_thread(self.control, self.command.puller())
        self.control_event_handler.error.connect(self.on_error)
        self.control_event_handler.model_changed.connect(self.on_model_changed)

    def new_main_frame(self, widget):
        self.main.removeWidget(self.main.currentWidget())
        self.main.addWidget(widget)
        self.main.setCurrentWidget(widget)

    def on_open(self, path):
        self.command.send({
            'command': 'load',
            'path': path,
        })

    def on_import(self):
        self.command.send({
            'command': 'import',
        })

    def on_reload(self):
        self.command.send({
            'command': 'reload',
        })

    def on_model_changed(self):
        self.system = self.control.system
        self.tree.load()

    def on_browser_selection_changed(self, current, previous):
        if isinstance(current, DateItem):
            widget = ThumbView()
            self.new_main_frame(widget)
            query = entry.EntryQuery(date=current.date.date)
            feed = entry.get_entries(query)
            widget.populate(feed)

    def on_message(self, state):
        self.state_label.setText(state)

    def on_error(self, message):
        W.QMessageBox.information(self, 'Error', message)

    def closeEvent(self, event):
        self.command.send({'command': 'quit'})


class ModelEventHandler(Background):
    message = C.pyqtSignal(str)
    error = C.pyqtSignal(str)

    def on_state(self, message):
        self.message.emit(message)

    def on_system(self, message):
        if message == 'quit':
            self.running = False
            self.quit_and_wait()

    def on_error(self, message):
        self.error.emit(message)


class ControlEventHandler(Background):
    enable = C.pyqtSignal(bool)
    error = C.pyqtSignal(str)
    model_changed = C.pyqtSignal()

    def __init__(self, control, *args):
        super().__init__(*args)

        self.control = control
        self.running = True

    def on_message(self, message):
        if message['command'] == 'load':
            try:
                self.control.load_config(message['path'])
                self.model_changed.emit()
            except ValueError as e:
                self.error.emit(str(e))
        if message['command'] == 'reload':
            self.model_changed.emit()
        elif message['command'] == 'import':
            try:
                logging.info('Importing...')
                from images7.importer import trig_import
                trig_import()
                self.model_changed.emit()
                logging.info('Imported.')
            except ValueError as e:
                self.error.emit(str(e))
        elif message['command'] == 'quit':
            self.running = False


class Control:
    def __init__(self, system):
        self.system = system

    def load_config(self, path):
        config = Config(path)
        self.system = System(config)
        importer.App.run(workers=1)
        job.App.run(workers=4)



if __name__ == '__main__':
    app = W.QApplication(sys.argv)
    main = View()
    logging.getLogger().setLevel(logging.DEBUG)
    sys.exit(app.exec_())
