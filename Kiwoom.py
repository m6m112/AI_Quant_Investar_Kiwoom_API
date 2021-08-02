from PyQt5.QAxContainer import *
from PyQt5.QtCore import QEventLoop

class Kiwoom(QAxWidget):                                    #QAxWidget(Qt+ActiveX+Widget) 클래스 상속  
    def __init__(self):                                     #클래스 초기 함수
        super().__init__()                                  #부모의 초기값들을 실행
        
        print("Kiwoom class start")

        ######### event loop ##########                     #event loop: 종료버튼 클릭 전까지 실행할 수 있도록 함 
        self.login_event_loop = None
            #####################    

        self.get_kiwoom_instance()                          #키움 instance 불러오는 함수 
        self.event_slots()                                  #Connection 확인 함수 

        self.signal_login_commConnect()                     #Kiwoom API의 로그인 함수 실행     

    def get_kiwoom_instance(self):                          #ocx 불러오는 함수 
        self.setControl("KHOPENAPI.KHOpenAPICtrl.1")        #레지스트리에 ocx가 저장된 폴더 ID
    
    def event_slots(self):                                  #event 함수 
        self.OnEventConnect.connect(self.login_slot)        #Connect 이벤트 발생 시, login_slot 함수로 연결(connect)
    
    def login_slot(self, errCode):                          #errCode 출력
        if errCode == 0:                                    
            print("connected")                                      
        else:
            print("disconnected")
        
        self.login_event_loop.exit()

    def signal_login_commConnect(self):                 
        self.dynamicCall("CommConnect()")                   #dynamicCall : 다른 서버 응용프로그램에 데이터 전송할 수 있도록 함
                                                            #CommConnect : Kiwoom 로그인 함수 
                                                            #--> kiwoom 로그인 함수를 사용 

        self.login_event_loop = QEventLoop()                #(로그인)이벤트 루프 객체 생성 
        self.login_event_loop.exec_()                       #이벤트 루프 생성 : 다음 실행 완료 시까지, 코드 진행 x --> 오류 방지 