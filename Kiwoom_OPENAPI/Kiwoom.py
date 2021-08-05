from PyQt5.QAxContainer import *
from PyQt5.QtCore import QEventLoop
from config.errorCode import *

class Kiwoom(QAxWidget):                                    #QAxWidget(Qt+ActiveX+Widget) 클래스 상속  
    def __init__(self):                                     #클래스 초기 함수
        super().__init__()                                  #부모의 초기값들을 실행
        
        print("Kiwoom class start")

        ######### event loop ##########                     #event loop: 종료버튼 클릭 전까지 실행할 수 있도록 함 
        self.login_event_loop = None                        #로그인 루프 
        self.detail_account_info_event_loop = None          #계좌 정보 루프 
            #####################    

        ########### 변수 ###########                        
        self.account_num = None                             #계좌번호 변수 
            ####################

        ########## 실행 함수 ###########                      
        self.get_kiwoom_instance()                          #키움 instance 불러오는 함수 
        self.event_slots()                                  #Connection 확인 함수 
        
        self.signal_login_commConnect()                     #Kiwoom API의 로그인 함수 실행     
        self.get_account_info()                             #계좌 정보 로드 함수 
        self.detail_account_info()                          #예수금 로드 함수 
        ###############################

    def get_kiwoom_instance(self):                          #레지스트리에 저장된 API 모듈 불러오는 함수# 
        self.setControl("KHOPENAPI.KHOpenAPICtrl.1")        #레지스트리에 모듈이 저장된 폴더 ID
    
    def event_slots(self):                                  #event 함수# 
        self.OnEventConnect.connect(self.login_slot)        #Connect(로그인) 이벤트 발생 시, login_slot 함수로 반환 값 연결(connect)   
        self.OnReceiveTrData.connect(self.trdata_slot)      #TRdata 요청 이벤트 발생 시, trdata_slot 함수로 반환 값 연결
                         
    def signal_login_commConnect(self):                     #로그인 시도 함수#
        self.dynamicCall("CommConnect()")                   #dynamicCall : 다른 서버 응용프로그램에 데이터 전송할 수 있도록 함
                                                            #CommConnect : Kiwoom 로그인 함수 
                                                            #--> kiwoom 로그인 함수를 사용 

        self.login_event_loop = QEventLoop()                #(로그인)이벤트 루프 객체 생성 
        self.login_event_loop.exec_()                       #이벤트 루프 생성 : 다음 실행 완료 시까지, 코드 진행 x --> 오류 방지 
    
    def login_slot(self, errCode):                          #errCode 출력 함수#
        
        print(errors(errCode))                              #errCode(번호)를 넘겨서 해당 error 출력
        
        self.login_event_loop.exit()                        #로그인 완료 후, loop 종료
    
    def get_account_info(self):                             #계좌 번호 로드 함수#
        account_list = self.dynamicCall("GetLoginInfo(QString)", "ACCNO")    
                                                            #계좌 list 불러오기
        account_num = account_list.split(';')[0]       #첫 번째[0] 계좌번호를 self 변수에 추가, 계좌 list가 ';'를 기준으로 나눠져 있음 
        self.account_num = account_num

        print("사용자의 보유 계좌번호 %s" % self.account_num) #보유 계좌번호 출력 (8005330511)
    
    def detail_account_info(self):                          #계좌 정보 요청 함수#
        self.dynamicCall("SetInputValue(QString, QString)", "계좌번호", self.account_num)   
        self.dynamicCall("SetInputValue(QString, QString)", "비밀번호", "0000")
        self.dynamicCall("SetInputValue(QString, QString)", "비밀번호입력매체구분", "00")
        self.dynamicCall("SetInputValue(QString, QString)", "조회구분", "2")         
                                                            #SetInputValue :  String 값 2개 계좌 input
        self.dynamicCall("CommRqData(QString, QString, int, QString)", "예수금상세현황요청","opw00001","0","2000")
                                                            #CommRqData : data 요청 / 요청명(사용자가 정함), TRkey(고유번호),preNext,화면번호

        self.detail_account_info_event_loop = QEventLoop()                 #event loop 생성
        self.detail_account_info_event_loop.exec_()                        #event loop 시작

    def trdata_slot(self, sScrNo, sRQName, sTrCode, sRecordName, sPrevNext):    #TrData 출력 함수#
        
        '''
        sScrNo: 스크린번호, sRQName: 요청명, sTrCode: tr코드, sRecordName: 사용안함, sPrevNext: 다음페이지 유무 
        '''
        
        if sRQName == "예수금상세현황요청":                  #요청 명령이 <예수금상세현황>일 경우, 
            deposit = self.dynamicCall("GetCommData(QString, QString, int, QString)", sTrCode, sRQName, 0, "예수금") 
            print("예수금 %s" % int(deposit))               #GetCommData: TRData 반환 , 예수금 반환 요청 (int : 형변환 , 000050000 -> 50000)
                                   
            ok_deposit = self.dynamicCall("GetCommData(QString, QString, int, QString )", sTrCode, sRQName, 0, "출금가능금액")
            print("출금가능금액 %s" % int(ok_deposit))       #출금가능금액 반환 요청 
        
        self.detail_account_info_event_loop.exit()          #이벤트 루프 종료 