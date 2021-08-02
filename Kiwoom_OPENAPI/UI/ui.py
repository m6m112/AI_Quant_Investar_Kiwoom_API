from Kiwoom import *
from PyQt5.QtWidgets import *

import sys

class UIClass():                        
    def __init__(self):
        print("UI_Class")

        self.app = QApplication(sys.argv)

        self.kiwoom = Kiwoom()

        self.app.exec_()
