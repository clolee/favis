import requests
import bs4
import sys
import datetime
import json
from collections import namedtuple
"""
Get ISIN CODE from KRX
"""
def getIsinCd(stock_name):
    url = "http://marketdata.krx.co.kr/WEB-APP/autocomplete/autocomplete.jspx?contextName=stkisu3&value="
    r = requests.get(url + stock_name)
    try:
        doc = bs4.BeautifulSoup(r.text, "html5lib")
        isu_cd = doc.find('li')['data-cd']
        isu_srt_cd = doc.find('li')['data-tp']
        return isu_cd, isu_srt_cd
    except TypeError as e:
        return '',''

def getIsinCode(stock_code):
    PREFIX='KR7'
    SUFFIX='00'
    isin = ''
    return PREFIX+stock_code+SUFFIX+str(calc_checkdigit(stock_code))
    
def calc_checkdigit(isin):
    #Convert alpha characters to digits
    isin2 = []
    for char in isin:
        if char.isalpha():
            isin2.append((string.ascii_uppercase.index(char.upper()) + 9 + 1))
        else:
            isin2.append(char)
    #Convert each int into string and join
    isin2 = ''.join([str(i) for i in isin2])
    #Gather every second digit (even)
    even = isin2[::2]
    #Gather the other digits (odd)
    odd = isin2[1::2]
    #If len(isin2) is odd, multiply evens by 2, else multiply odds by 2
    if len(isin2) % 2 > 0:
        even = ''.join([str(int(i)*2) for i in list(even)])
    else:
        odd = ''.join([str(int(i)*2) for i in list(odd)])
    even_sum = sum([int(i) for i in even])
    #then add each single int in both odd and even
    odd_sum = sum([int(i) for i in odd])
    mod = (even_sum + odd_sum) % 10
    return (10-mod)%10


# get krx daily info 
def get_krx_daily_info(isu_cd, start, end):
    
    # STEP 01: Generate OTP
    gen_otp_url = 'http://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx'
    gen_otp_data = {
        'name':'fileDown',
        'filetype':'xls',
        'url':'MKD/04/0402/04020100/mkd04020100t3_02',
        'isu_cd':isu_cd,
        'fromdate':start,
        'todate':end
    }

    r = requests.get(gen_otp_url, gen_otp_data)
    code = r.text

    headers = { 'Referer': 'http://marketdata.krx.co.kr/mdi', 'Connection': 'close'}
    # STEP 02: download
    down_url = 'http://file.krx.co.kr/download.jspx'
    down_data = {
        'code': code,
    }

    return requests.post(down_url, down_data, headers=headers).content

# get krx daily info 
def get_krx_daily_price(isu_cd, start, end, filetype):
    
    if not filetype:
        filetype = 'xls'
    # STEP 01: Generate OTP
    gen_otp_url = 'http://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx'
    gen_otp_data = {
        'name':'fileDown',
        'filetype':filetype,
        'url':'MKD/04/0402/04020100/mkd04020100t3_02',
        'isu_cd':isu_cd,
        'fromdate':start,
        'todate':end
    }
    headers = { 'Referer': 'http://marketdata.krx.co.kr/mdi', 'Connection': 'close'}

    r = requests.post(gen_otp_url, gen_otp_data, headers=headers)
    code = r.text

    # STEP 02: download
    down_url = 'http://file.krx.co.kr/download.jspx'
    down_data = {
        'code': code,
    }

    return requests.post(down_url, down_data, headers={'Connection':'close'}).content
    
    
def get_krx_today_info(stock_code, isu_cd):
    # STEP 01: Generate OTP
    gen_otp_url = 'http://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx'
    gen_otp_data = {
        'name':'tablesubmit',
        'bld':'MKD/04/0402/04020100/mkd04020100t2_01' # get today price info
    }
    
    r = requests.post(gen_otp_url, gen_otp_data)
    code = r.text

    today = datetime.datetime.today().strftime('%Y%m%d')
    # STEP 02: download
    down_url = 'http://marketdata.krx.co.kr/contents/MKD/99/MKD99000001.jspx'
    down_data = {
            'code':code,
            'bldcode':'MKD/04/0402/04020100/mkd04020100t2_01',
            'isu_cd':isu_cd,
            'fromdate':today,
            'todate':today
    }
    
    r = requests.post(down_url, down_data).text
    
    x = json.loads(r, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
    # stock_code, date, open, high, low, close, volume, marcap, amount
    ret = dict()
    ret['stock_code'] = stock_code
    ret['date'] = today
    ret['open'] = x.block1[0].isu_opn_pr.replace(',','')
    ret['high'] = x.block1[0].isu_hg_pr.replace(',','')
    ret['low'] = x.block1[0].isu_lw_pr.replace(',','')
    ret['close'] = x.block1[0].isu_cur_pr.replace(',','')
    ret['volume'] = x.block1[0].isu_tr_vl.replace(',','')
    ret['marcap'] = x.block1[0].isu_tr_amt.replace(',','')
    ret['amount'] = ''
    return ret
    
def get_krx_daily_sellbuy_trend(isu_cd, start, end):    
    # get krx daily info 
    # STEP 01: Generate OTP
    gen_otp_url = 'http://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx'
    gen_otp_data = {
        'name':'fileDown',
        'filetype':'xls',
        'url':'MKD/10/1002/10020103/mkd10020103_01',
        'isu_cd':isu_cd,
        'type':'D',
        'period_strt_dd':start,
        'period_end_dd':end
    }

    headers = { 'Referer': 'http://marketdata.krx.co.kr/mdi', 'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36'}

    r = requests.post(gen_otp_url, gen_otp_data, headers=headers)
    code = r.text

    print(r.text)

    # STEP 02: download
    down_url = 'http://file.krx.co.kr/download.jspx'
    down_data = {
        'code': code,
    }
    headers = { 'Referer': 'http://marketdata.krx.co.kr/mdi', 'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36'}
    return requests.post(down_url, down_data, headers=headers).content

	
def get_krx_sellbuy_detail(isu_cd, start, end):
    
    # STEP 01: Generate OTP
    gen_otp_url = 'http://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx'
    gen_otp_data = {
        'name':'fileDown',
        'filetype':'xls',
        'url':'MKD/10/1002/10020101/mkd10020101',
        'isu_cd':isu_cd,
        'period_selector':'day',
        'fromdate':start,
        'todate':end
    }

    headers = { 'Referer': 'http://marketdata.krx.co.kr/mdi', 'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36'}
    r = requests.post(gen_otp_url, gen_otp_data, headers=headers)
    code = r.text

    # STEP 02: download
    down_url = 'http://file.krx.co.kr/download.jspx'
    down_data = {
        'code': code,
    }
    return requests.post(down_url, down_data, headers=headers).content
    
def get_krx_daily_stock_index(isu_cd, isu_srt_cd, start, end):    
    # get krx daily info 
    # STEP 01: Generate OTP
    gen_otp_url = 'http://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx'
    gen_otp_data = {
        'name':'fileDown',
        'filetype':'xls',
        'url':'MKD/04/0403/04030800/mkd04030800',
        'isu_cd':isu_cd,
        'market_gubun':'ALL',
        'gubun':'2',
		'isu_srt_cd':isu_srt_cd,
        'fromdate':start,
        'todate':end
    }
    
    r = requests.post(gen_otp_url, gen_otp_data)
    code = r.text
    
    # STEP 02: download
    down_url = 'http://file.krx.co.kr/download.jspx'
    down_data = {
        'code': code,
    }
    headers = { 'Referer': 'http://marketdata.krx.co.kr/mdi'}
    
    return requests.post(down_url, down_data, headers=headers).content

if __name__ == '__main__':
    isu_cd, isu_srt_cd = getIsinCd("경동도시가스")
    print (isu_cd, isu_srt_cd)
        
        
        
        
