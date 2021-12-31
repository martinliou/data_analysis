# -*- coding: utf-8 -*-
import unittest
import crawler
import pytz
from datetime import date, datetime

# constants and utility objects
TAIPEI_TIMEZONE = pytz.timezone("Asia/Taipei")
ASSET_PATH = 'assets/'


class TestCap(unittest.TestCase):
    def setUp(self):  # called when a case is started
        pass

    def tearDown(self):  # called when a case is done
        pass

    def test_date_to_query_date_str(self):
        expected = '109/08/20'
        result = crawler.date_to_query_date_str(date(2020, 8, 20))
        self.assertEqual(result, expected)

    def test_arg_to_query_date_str(self):
        expected = '109/08/20'
        result = crawler.arg_to_query_date_str('20200820')
        self.assertEqual(result, expected)

    def test_query_date_str_to_datetime(self):
        expected = TAIPEI_TIMEZONE.localize(datetime(2020, 8, 20))
        result = crawler.query_date_str_to_datetime('109/08/20')
        self.assertEqual(result, expected)

    def test_roc_to_datetime(self):
        expected = TAIPEI_TIMEZONE.localize(datetime(2020, 8, 20))
        result = crawler.roc_to_datetime('109/08/20')
        self.assertEqual(result, expected)

    def test_datetime_to_timestamp(self):
        expected = 1599062400000000
        result = crawler.datetime_to_timestamp(
            TAIPEI_TIMEZONE.localize(datetime(2020, 9, 3)))
        self.assertEqual(result, expected)

    def test_xxx(self):
        expected = [[1598976000000000,
                     '證櫃債字第10900106611號',
                     'https://www.tpex.org.tw/web/bulletin/announcement/ann_detail.php?l=zh-tw&content_file=MTA5MDAxMDY2MTEuaHRtbA%3D%3D&content_number=MTA5MDAxMDY2MTE%3D'],
                    [1598976000000000,
                     '證櫃債字第10900107951號',
                     'https://www.tpex.org.tw/web/bulletin/announcement/ann_detail.php?l=zh-tw&content_file=MTA5MDAxMDc5NTEuaHRtbA%3D%3D&content_number=MTA5MDAxMDc5NTE%3D'],
                    [1598976000000000,
                     '證櫃視字第10912026751號',
                     'https://www.tpex.org.tw/web/bulletin/announcement/ann_detail.php?l=zh-tw&content_file=MTA5MTIwMjY3NTEuaHRtbA%3D%3D&content_number=MTA5MTIwMjY3NTE%3D']]
        with open(ASSET_PATH + '/20200902.html', 'rt') as fin:
            result = crawler.parse_summary(crawler.parse_html(fin.read()))
        self.assertEqual(result, expected)

    def test_parse_html_content_tag_1(self):
        expected = (
            '「鴻海精密工業股份有限公司109年度第2期無擔保普通公司債甲券~丁券」計面額新台幣82.5億元整，訂於109年9月9日起在證券商營業處所暨本中心債券等殖成交系統開始買賣。',
            '本中心109年8月31日證櫃債字第10900106621號函暨上開公司109年9月1日公司債櫃檯買賣申報書。',
            [
                (1, '一、發行公司名稱：鴻海精密工業股份有限公司。'),
                (1, '二、債券名稱：鴻海精密工業股份有限公司109年度第2期無擔保普通公司債甲券。'),
                (2, '(一)代碼：B644BR。'),
                (2, '(二)簡稱：P09鴻海3A。'),
                (2, '(三)發行總面額：新台幣28.5億元整。'),
                (2, '(四)發行價格(佰元價)：新台幣100元(依票面金額100%發行)。'),
                (2, '(五)發行日：109年9月9日。'),
                (2, '(六)到期日：114年9月9日。'),
                (2, '(七)發行期限：5年。'),
                (2, '(八)票面利率：固定利率0.69%。'),
                (1, '三、債券名稱：鴻海精密工業股份有限公司109年度第2期無擔保普通公司債乙券。'),
                (2, '(一)代碼：B644BS。'),
                (2, '(二)簡稱：P09鴻海3B。'),
                (2, '(三)發行總面額：新台幣37億元整。'),
                (2, '(四)發行價格(佰元價)：新台幣100元(依票面金額100%發行)。'),
                (2, '(五)發行日：109年9月9日。'),
                (2, '(六)到期日：116年9月9日。'),
                (2, '(七)發行期限：7年。'),
                (2, '(八)票面利率：固定利率0.79%。'),
                (1, '四、債券名稱：鴻海精密工業股份有限公司109年度第2期無擔保普通公司債丙券。'),
                (2, '(一)代碼：B644BT。'),
                (2, '(二)簡稱：P09鴻海3C。'),
                (2, '(三)發行總面額：新台幣14億元整。'),
                (2, '(四)發行價格(佰元價)：新台幣100元(依票面金額100%發行)。'),
                (2, '(五)發行日：109年9月9日。'),
                (2, '(六)到期日：119年9月9日。'),
                (2, '(七)發行期限：10年。'),
                (2, '(八)票面利率：固定利率0.90%。'),
                (1, '五、債券名稱：鴻海精密工業股份有限公司109年度第2期無擔保普通公司債丁券。'),
                (2, '(一)代碼：B644BU。'),
                (2, '(二)簡稱：P09鴻海3D。'),
                (2, '(三)發行總面額：新台幣3億元整。'),
                (2, '(四)發行價格(佰元價)：新台幣100元(依票面金額100%發行)。'),
                (2, '(五)發行日：109年9月9日。'),
                (2, '(六)到期日：121年9月9日。'),
                (2, '(七)發行期限：12年。'),
                (2, '(八)票面利率：固定利率1.0%。'),
                (1, '六、債券銷售對象：僅限售予「財團法人中華民國證券櫃檯買賣中心外幣計價國際債券管理規則」所定之專業投資人。'),
                (1, '七、有關本債券之詳細發行資料請參閱其發行辦法及公開說明書(請至公開資訊觀測站https://mops.twse.com.tw查詢)。'),
                (1, '八、上開債券於到期日之前二個營業日停止於本中心債券等殖成交系統買賣，並於到期之次一個營業日終止於證券商營業處所買賣。')
            ]
        )
        with open(ASSET_PATH + '/證櫃債字第10900108531號.html', 'rt') as fin:
            result = crawler.parse_html_content_tag(
                crawler.parse_html(fin.read()))
        self.assertEqual(result, expected)

    def test_parse_html_content_tag_2(self):
        expected = (
            '聯德控股股份有限公司中華民國境內第二次無擔保轉換公司債(債券簡稱：聯德控股二KY；代號：49122)訂於109年9月8日終止櫃檯買賣。',
            '本中心證券商營業處所買賣有價證券業務規則第3條第3項及聯德控股股份有限公司109年9月2日於公開資訊觀測站辦理轉換公司債已全數轉換普通股或已由公司買回或償還時下櫃之公告。',
            [
                (0, '聯德控股股份有限公司中華民國境內第二次無擔保轉換公司債因已由公司買回，訂於109年9月8日終止櫃檯買賣。')
            ]
        )
        with open(ASSET_PATH + '/證櫃債字第1090400615號.html', 'rt') as fin:
            result = crawler.parse_html_content_tag(
                crawler.parse_html(fin.read()))
        self.assertEqual(result, expected)

    def test_parse_html_content_tag_3(self):
        expected = (
            '撼訊科技股份有限公司國內第四次無擔保轉換公司債(債券簡稱：撼訊四；代號：61504)因發行公司行使贖回權，訂於109年10月19日終止櫃檯買賣。',
            '本中心證券商營業處所買賣有價證券業務規則第3條第3項及撼訊科技股份有限公司109年8月31日於公開資訊觀測站辦理轉換公司債強制贖回或到期並下櫃之公告。',
            None)
        with open(ASSET_PATH + '/證櫃債字第1090400611號.html', 'rt') as fin:
            result = crawler.parse_html_content_tag(
                crawler.parse_html(fin.read()))
        self.assertEqual(result, expected)


if __name__ == '__main__':
    unittest.main()
