import pymysql 
import pandas as pd
from datetime import datetime
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
            rs = curs.fetchone()                                     #한 줄씩 가져옴 
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

if __name__ == '__main__':                      #main 함수를 의미
    dbu = DBUpdater()                           #DBUpdater 객체 생성 
    dbu.update_comp_info()                      #dbu객체의 updqte_comp_info 함수 실행 

