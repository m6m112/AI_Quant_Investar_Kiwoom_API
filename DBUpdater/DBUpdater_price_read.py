import pymysql, json, calendar 
import pandas as pd
from bs4 import BeautifulSoup
from urllib.request import urlopen
from datetime import datetime
from threading import Timer

class DBUpdater:
    def __init__(self):     #초기화 함수 - self 자기 자신을 의미함 
        """생성자: MariaDB 연결 및 종목코드 딕셔너리 생성"""
        self.conn = pymysql.connect(host="localhost", user='root', password = '1234', db = 'INVESTAR', charset ='utf8' ) #db 연결 

        with self.conn.cursor() as curs:            #self.conn.cursor을 불러와서 curs로 사용한다. cursor : sql문을 실행하기 위해 필요함 
            sql = """
            CREATE TABLE IF NOT EXISTS company_info(
                code VARCHAR(20),
                company VARCHAR(20),
                last_update DATE,
                PRIMARY KEY (code))
            """                                     #sql문 작성 
            curs.execute(sql)                       #sql문 전달 및 실행 
            
            sql = """
            CREATE TABLE IF NOT EXISTS daily_price (
                code VARCHAR(20),
                date DATE,
                open BIGINT(20),
                high BIGINT(20),
                low BIGINT(20),
                close BIGINT(20),
                diff BIGINT(20),
                volume BIGINT(20),
                PRIMARY KEY (code, date))
            """                                 #sql문 작성
            curs.execute(sql)                   #sql문 전달 및 실행
        self.conn.commit()                      #DB 최종 저장 

        self.codes = dict()                     #dictionary 생성
        self.update_comp_info()                 #함수 실행 
    
    def __del__(self):
        """소멸자: MariaDB 연결 해제 """
        self.conn.close()
    
    def read_krx_code(self):
        """KRX로부터 상장기업 목룍 파일을 읽어와서 데이터프레임으로 반환"""
        url='http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13'
        krx = pd.read_html(url, header=0)[0]       #상장법인목록 파일 읽기 (모든 테이블을 가져오는데, 0번째 테이블을 쓰겠다는 의미)
        krx = krx[['종목코드', '회사명']]           #종목코드와 회사명만 남기고 원하는 순서로 재구성 
        krx = krx.rename(columns={'종목코드':'code', '회사명':'company'})        #한글 칼럼명 -> 영문 칼럼명 변경 
        krx.code = krx.code.map('{:06d}'.format)                               #종목코드를 6자리의 숫자, 빈 자리는 0으로 채우는 형식으로 반환 
        return krx

    def update_comp_info(self):
        """종목코드를 company_info 테이블에 업데이트 한 후 딕셔너리에 저장"""
        sql = "SELECT * FROM company_info"     #company_info를 선택한다는 sql문 
        df = pd.read_sql(sql, self.conn)       #테이블을 읽음
        
        for idx in range(len(df)):
            self.codes[df['code'].values[idx]] = df['company'].values[idx]          #종목코드 딕셔너리에 종목코드와 회사 연결해서 저장 
        
        with self.conn.cursor() as curs:            #curs로 cursor를 불러옴
            sql = "SELECT max(last_update) FROM company_info"        #company_info에서 last_update값을 불러옴 
            curs.execute(sql)                                        #sql문 실행
            rs = curs.fetchone()                                     #한 데이터만  가져옴 
            today = datetime.today().strftime('%Y-%m-%d')            #오늘 날짜 불러옴 strftime : 날짜 및 시간을 스트링으로 변환
            
            
            if rs[0] == None or rs[0].strftime('%Y-%m-%d') < today:  #업데이트가 안된 경우 
                krx = self.read_krx_code()                           #상장 목록 파일 불러오기 
                
                for idx in range(len(krx)):
                    code = krx.code.values[idx]                      #각각의 종목 코드 로드 
                    company = krx.company.values[idx]                #각각의 회사명 로드 
                    sql =  f"REPLACE INTO company_info (code, company, last_update) VALUES ('{code}','{company}','{today}')"         #각 데이터를 업데이트(or insert)하라는 sql문
                    curs.execute(sql)                                #sql문 실행 
                    self.codes[code] = company                       #딕셔너리 업데이트 
                    tmnow = datetime.now().strftime('%Y-%m-%d %H:%M') #현재 시간 로드 
                    print(f"[{tmnow}] #{idx+1:04d} REPLACE INTO company_info VALUES ({code}, {company}, {today})")                      #업데이트 완료문 출력 
                
                self.conn.commit()                                   #DB 최종 저장   

    def read_naver(self, code, company, pages_to_fetch):
        """네이버에서 주식 시세를 읽어서 데이터프레임으로 반환"""
        try:
            url = f"http://finance.naver.com/item/sise_daty.nhn?code={code}"        #네이버 금융 사이트 불러오기 
            with urlopen(url) as doc:                                               #url열기
                if doc is None:                                                     #아무것도 없으면
                    return None                                                     #종료

                html = BeautifulSoup(doc,"lxml")                                    #doc(웹페이지)를 lxml파일 형식으로 파싱
                pgrr = html.find("td", class_="pgRR")                               #class명이 pgRR인 td태그 가져오기 
                if pgrr is None:                                                    #없으면 
                    return None                                                     #종료

                s = str(pgrr.a["href"]).split('=')                                  #td태그의 하위 태그인 a의 속성 값인 href를 '='단위로 분해하여 s에 저장
                lastpage = s[-1]                                                    #마지막 원소를 lastpage에 저장 (href의 마지막 '='의 다음 값이 마지막 page를 가리킴)
            
            df = pd.DataFrame()                                                     #새로운 데이터프레임 생성
            pages = min(int(lastpage), pages_to_fetch)                              #lastpage와 pages_to_fetch(함수의 원소) 중 더 작은 값을 pages로 저장     

            for page in range(1, pages + 1):                                        #페이지 수 만큼 반복
                pg_url = '{}&page={}'.format(url, page)                             #page별 url따기 -> format : 대괄호에 들어갈 값을 연결     
                df = df.append(pd.read_html(pg_url, header=0)[0])                   #각 page를 df에 저장
                tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')                   #현재 시간 저장 
                print('[{}] {} ({}) : {:04d}/{:04d} pages are downloading...'.format(tmnow, company, code, page, pages), end="\r")      #현재 시간에 어디까지 다운로드 했는지 저장
                                                                                                                        #end : 마지막에 출력할 스트링 , "\r" : 커서를 맨 앞으로 보냄 -> 같은 위치에서 출력 반복  
            
            df = df.rename(colunms={'날짜':'date','종가':'close','전일비':'diff','시가':'open','고가':'high','저가':'low','거래량':'volume'})    #각 칼럼의 이름을 영문으로 바꿈
            df['date'] = df['date'].replace('.','-')                                                                                          #데이터의 '.'를 '-'로 바꿈
            df = df.dropna()                                                        #dropna : 결측값 제거, dropna() : 결측값 있는 전체 행 삭제 
            df[['close','diff','open','high','low','volume']] = df[['close','diff','open','high','low','volume']].astype(int)                 #각 값을 int형으로 변경 (DB에 BIGINT형으로 지정했기 때문에)
            df = df[['date','open','high','low','close','diff','volume']]                                                                     #원하는 순서대로 칼럼 재조합   
        
        except Exception as e:                                                      #에러 발생 시, 에러 출력 
            print('Exception occured :', str(e))
            return None
        
        return df                                                                   #데이터프레임 반환
    
    def replace_into_db(self, df, num, code, company):                              
        """네이버에서 읽어온 주식 시세를 DB에 REPLACE"""
        with self.conn.cursor() as curs:                                            #cursor(db접근 시, 필요) 불러오기 
            for r in df.intertuples():                                              #intertuples(): 튜플 형식으로 한 줄씩 불러옴
                sql = f"REPLACE INTO daliy_price VALUES ('{code}','{r.date}','{r.open}','{r.high}','{r.low}','{r.close}','{r.diff}','{r.volume}')" #값을 바꾼다는 sql 문
                curs.execute(sql)                                                   #sql문 실행
            self.conn.commit()                                                      #최종 db 저장 
            print('[{}] #{:04d} {} ({}) : {} rows > REPLACE INTO daily_price[OK]'.format(datetime.now().strftime('%Y-%m-%d %H:%M'), num+1, company, code, len(df)))     #출력문 

    def update_daily_price(self, pages_to_fetch):
        """KRX 상장법인의 주식 시세를 네이버로부터 읽어서 DB에 업데이트"""
        for idx, code in enumerate(self.codes):                                     #enumerate(): 몇 번째 출력인지(index)를 함께 반환해줌                                 
            df = self.read_naver(code, self.codes[code], pages_to_fetch)            #일별 시세 데이터 프레임 반환
            if df is None:                                                          #데이터 없으면 다시 for문 반복            
                continue
            self.replace_into_db(df, idx, code, self.codes[code])                   #db 업데이트 

    def execute_daily(self):
        """실행 즉시 및 매일 오후 5시에 daily_price 테이블 업데이트"""
        self.update_comp_info()                                                     #daily_price 업데이트

        try:                                                                        #실행 (예외처리 시, 사용)
            with open('config.json','r') as in_file:                                #config.json 파일을 읽기 모드로 open --> in_file의 이름으로 
                config = json.load(in_file)                                         #json파일을 읽어 dict타입으로 저장 
                pages_to_fetch = config['pages_to_fetch']                           #pages_to_fetch 요소를 따로 저장 

        except FileNotFoundError:                                                   #예외 발생 시, (파일이 없는 경우)
            with open('config.json','w') as out_file:                               #config.json 파일을 쓰기 모드로 open --> out_file의 이름으로 
                pages_to_fetch = 100                                                #초기 값을 100으로 설정 
                config = {'pages_to_fetch' : 1}                                     #config의 데이터 1로 설정 (최초 업데이트한 후에는 1장씩 업데이트 하기 위해)
                json.dump(config, out_file)                                         #config 값을 json파일로 변경하여 out_file에 저장 

        self.update_daily_price(pages_to_fetch)                                     #pages_to_fetch에 맞춰 가격 업데이트

        tmnow = datetime.now()                                                      #현재 년도, 날짜, 시간 저장
        lastday = calendar.monthrange(tmnow.year, tmnow.month)[1]                   #이번 달의 마지막 날 반환 
        if tmnow.month == 12 and tmnow.day == lastday:                              #이번 년도의 마지막 날 인 경우, 
            tmnext = tmnow.replace(year = tmnow.year+1, month = 1 , day = 1, hour = 17, minute = 0, second = 0)                 #next는 다음 해, 1월 1일 17시로 저장
        elif tmnow.day == lastday:                                                  #이번 달의 마지막 날 인 경우, 
            tmnext = tmnow.replace(month = tmnow.month+1 , day = 1, hour = 17, minute = 0, second = 0)                          #next는 다음 달, 1일 17시로 저장
        else:                                                                       #마지막 달, 마지막 날 모두 아닌 경우, 
            tmnext = tmnow.replace(day = tmnow.day+1, hour = 17, minute = 0, seconde = 0)                                       #next는 다음 날, 17시로 저장
        tmdiff = tmnext - tmnow                                                     #현재 시간과 다음 시간의 차이 반환
        secs = tmdiff.seconds                                                       #차이 시간 (초) 저장 

        t = Timer(secs, self.execute_daily)                                         #차이 시간 후에  execute_daily 함수 실행
        print("Waiting for next update({}) ...".format(tmnext.strftime('%Y-%m-%d %H:%M')))          #다음 업데이트 시간 알림 출력 
        t.start()                                                                   #타이머 다시 시작 (매 시간마다 실행하기 위해서)

if __name__ == '__main__':                      #main 함수를 의미
    dbu = DBUpdater()                           #DBUpdater 객체 생성 
    dbu.execute_daily()                      #dbu객체의 updqte_comp_info 함수 실행 

