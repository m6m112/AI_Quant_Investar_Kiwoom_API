import pymysql, requests ,urllib3
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime

urllib3.disable_warnings()

"""
DB명 : Quant
시가총액 코스닥 300위 기업 
최근 1년간의 차트데이터 
체결일(date), 시가(open), 고가(high), 저가(low), 종가(close), 거래량(volume)
"""

class Chartdata:
    def __init__(self):                             #초기화함수 
        """생성자: MariaDB 연결 및 종목코드 딕셔너리 생성"""
        self.conn = pymysql.connect(host="localhost", user='root', password = "1234", db = "QUANT",charset="utf8")

        ############ 테이블 생성 ############
        with self.conn.cursor() as curs:           #cursor 연결 (DB 연결 커서)
            sql = """
            CREATE TABLE IF NOT EXISTS company_info(        
                code VARCHAR(20),
                company VARCHAR(20),
                last_update DATE,
                PRIMARY KEY (code))
            """                                     #회사정보 테이블 생성 sql문
            curs.execute(sql)

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
            """                                     #일별 가격 테이블 생성 sql문 
            curs.execute(sql)                       #sql문 실행 
            self.conn.commit()                      #DB 최종 저장 
        ###################################

        self.codes = dict()                         #종목코드를 담을 딕셔너리 생성 
        self.update_comp_info()                     #회사정보 업데이트 함수 실행 
    
    def __del__(self):                              #소멸자함수
        """소멸자: MariaDB 연결 해제 """
        self.conn.close()

    def read_comp_info(self):                                                                      
        """네이버 금융에서 코스피 시총 300위 기업명, 종목코드 데이터프레임 반환"""
        CompanyInfos = pd.DataFrame()                                                                                               #반환할 데이터프레임 생성  

        for page in range (1,7):                                                                                                    #1 ~ 6 page  -> 300위 기업 크롤링 
            try:
                url = "https://finance.naver.com/sise/sise_market_sum.nhn?sosok=1&page={}".format(page)                             #네이버 금융 시총 순 페이지 url
                html = BeautifulSoup(requests.get(url, verify=False, headers = {'User-agent' : 'Mozilla/5.0'}).text,"lxml")         #요청한 url 가져옴, parser --> lxml (html을 해석하는 패키지)
                titles = html.findAll("a", class_="tltle")                                                                          #a 태그 라인 중 class 명이 tltle인 라인을 전체 반환  
                
                if titles is None:                                                                                                                                                                                              
                    print("title is None")                                                                                           
                    return None   
                
                for idx, title in enumerate(titles,50*(page-1)+1):
                    company = title.get_text()                                                                                       #회사명 추출                                                       
                    s = str(title["href"]).split('=')                                                                                #하위 태그 a의 href를 텍스트 반환 -> '='기준으로 split 
                    codenum = s[-1]                                                                                                  #종목코드 추출 
                    
                    new_data = {'code':codenum, 'company':company}                                                                   #추출한 회사명, 종목코드 데이터프레임 형식으로 변환 
                    CompanyInfos = CompanyInfos.append(new_data,ignore_index=True)                                                   #회사명, 종목코드 데이터프레임에 저장 
                                                                                                                                     
                    print(f" #{idx} CompanyInfo UPDATE : {codenum} , {company}")                                                     #업데이트 현황 출력   
                   
            except Exception as e:                                                                                                   #예외 발생 시, 
                print('Exception occured: ', str(e))                                                                                 #출력     
                return None

        return CompanyInfos                                                                                                          #companyinfo 데이터프레임 반환 
                                                                                                                                        
    def update_comp_info(self):
        """종목코드와 기업명을 DB에 업데이트 """              
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
                CompanyInfos = self.read_comp_info()                 #상장 목록 파일 불러오기 
                
                for idx in range(len(CompanyInfos)):
                    code = CompanyInfos.code.values[idx]                      #각각의 종목 코드 로드 
                    company = CompanyInfos.company.values[idx]                #각각의 회사명 로드 
                    sql =  f"REPLACE INTO company_info (code, company, last_update) VALUES ('{code}','{company}','{today}')"         #각 데이터를 업데이트(or insert)하라는 sql문
                    curs.execute(sql)                                #sql문 실행 
                    self.codes[code] = company                       #딕셔너리 업데이트 
                    tmnow = datetime.now().strftime('%Y-%m-%d %H:%M') #현재 시간 로드 
                    print(f"[{tmnow}] #{idx+1:04d} REPLACE INTO company_info VALUES ({code}, {company}, {today})")                      #업데이트 완료문 출력 
                
                self.conn.commit()                                   #DB 최종 저장  

if __name__ == '__main__':
    dbu = Chartdata()                                                                                                     
    
    
  