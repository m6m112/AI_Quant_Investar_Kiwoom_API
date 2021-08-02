from UI.ui import *                     #ui에 있는 전체 class 불러오기


class Main():                           #클래스 명은 항상 대문자로 
    def __init__(self):                 #클래스 초기화 함수     
        print("Main class start")             

        UIClass()
if __name__ == "__main__":              #메인 함수임을 알려줌 
    Main()                              #메인 클래스 불러오기 
