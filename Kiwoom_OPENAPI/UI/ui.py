from Kiwoom import *
from PyQt5.QtWidgets import *

import sys

class UIClass():                        
    def __init__(self):
        print("UI_Class start")

        self.app = QApplication(sys.argv)       #메인 event loop 생성         

        self.kiwoom = Kiwoom()                  #Kiwoom Class 실행 

        self.app.exec_()                        #event loop 실행 
