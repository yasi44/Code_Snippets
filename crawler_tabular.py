# -*- coding: utf-8 -*-
"""
Author: YAS
Date: 2020 October 5
Project Name: WebCrawler

Description: Crawler for construction projects

"""

import time
from datetime import datetime
import pymongo
import pytz
import sqlalchemy as db
import yaml
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options as chrome_option
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options as firefox_options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from sqlalchemy import Column, Float, Integer, VARCHAR, DATE
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.dialects.mysql import insert
from exception_handling import print_exception, setup_logger
from selenium.webdriver.common.keys import Keys

# file logger
_logger = setup_logger('main_logger', 'log.log')


class CrawlerTabular:
    def __init__(self, name):
        try:
            # if sys.argv[1].startswith('http://'):
            #     res = requests.get(sys.argv[1])
            #     self.config = yaml.load(res.text)
            # else:
            #     with open(sys.argv[1]) as input_file:
            #         s = input_file.read()
            #         self.config = yaml.load(s , Loader=yaml.FullLoader)

            # temporary for test only
            with open('config.yml') as input_file:
                s = input_file.read()
                self.config = yaml.load(s, Loader=yaml.FullLoader)

            self._today = datetime.now()
            self.str_day = str(self._today.day)
            self.str_month = str(self._today.month)
            self.str_year = str(self._today.year)

        except Exception as e:
            _logger.error('Exception %s' % e)
            print_exception(e, exitStatus=True)
        self.target_website = name

    def setup(self):
        try:
            if self.config['name'] == self.target_website:
                try:

                    if self.config['driver'] == "chrome":
                        chrome_options = chrome_option()
                        chrome_options.add_argument("--disable-notifications")
                        chrome_options.add_argument('--headless')
                        chrome_options.add_argument('--no-sandbox')
                        # # chrome_options.add_argument('--disable-dev-shm-usage')
                        # chrome_options.add_argument('--enable-automation')
                        # # chrome_options.add_argument('--lang=en_US')
                        # chrome_options.add_argument("--allow-http-screen-capture")
                        # chrome_options.add_argument("--disable-impl-side-painting")
                        # chrome_options.add_argument("--disable-setuid-sandbox")
                        # chrome_options.add_argument("--disable-seccomp-filter-sandbox")
                        # chrome_options.add_argument("--disable-infobar")
                        # import getpass
                        # _user = getpass.getuser()
                        self.driver = webdriver.Chrome(executable_path=r'C:\Users\yasaman.eftekhary\PycharmProjects\all_crawlers\chromedriver.exe', options=chrome_options)
                        # self.driver = webdriver.Chrome(executable_path='../../chromedriver', options=chrome_options)

                    elif self.config['driver'] == "firefox":
                        _options = firefox_options()
                        _options.headless = True
                        self.driver = webdriver.Firefox(executable_path=r'C:\Users\yasaman.eftekhary\PycharmProjects\all_crawlers\geckodriver.exe',options=_options)
                        # self.driver = webdriver.Firefox(executable_path='../../chromedriver',options=_options)
                        print('g')
                except Exception as e:
                    _logger.error('Exception %s' % e)
                    print_exception(e, exitStatus=True)

                self.DB_url = self.config['DB']['url']
                self.DB_name = self.config['DB']['DB_name']
                self.DB_type = self.config['DB']['type']
                if self.DB_type == 'mongo':
                    _myclient = pymongo.MongoClient(self.DB_url)
                    _mydb = _myclient[self.DB_name]
                    self.DB_connection = _mydb[self.table]
                elif self.DB_type == 'mysql':
                    i = 0
                    self.engine = db.create_engine(self.DB_url + self.DB_name)
                    Session = sessionmaker(bind=self.engine)
                    self.session = Session(autocommit=True)
                    self.DB_connection = self.engine.connect()
                    # self.trans=self.DB_connection.begin()
                    metadata = db.MetaData()
                    self.table = db.Table(self.config['DB']['table'],
                                          metadata,
                                          autoload=True,
                                          autoload_with=self.engine)
        except Exception as e:
            _logger.error('Exception %s' % e)
            print_exception(e, exitStatus=True)

    def grab_content_one_page(self, status, area_code, key, val, area_name, first_idx):
        try:
            time.sleep(2)
            # --- evens
            num_trs = 0
            num_trs = len(self.driver.find_elements_by_xpath("//tr[@class='ui-widget-content ui-datatable-even']"))
            for tr in range(first_idx, num_trs):
                try:
                    _tds = self.driver.find_elements_by_xpath(
                        "//tr[@class='ui-widget-content ui-datatable-even']")[tr].find_elements_by_tag_name('td')
                    procurement_title = _tds[0].text
                    PTJ = _tds[1].text
                    date_of_advertisement = time.strptime(_tds[2].text, "%d/%m/%Y %H:%M %p")  # 05/10/2020 12:00 PM
                    closing_date = time.strptime(_tds[3].text, "%d/%m/%Y %H:%M %p")  # 05/10/2020 12:00 PM
                    ins = insert(self.table).values(
                        procurement_title=procurement_title,
                        PTJ=PTJ,
                        date_of_advertisement=date_of_advertisement,
                        closing_date=closing_date,
                        status=status,
                        area=area_name,
                        updated_date=self._today).prefix_with('IGNORE')

                    on_duplicate_key_stmt = ins.on_duplicate_key_update(
                        procurement_title=procurement_title,
                        PTJ=PTJ,
                        date_of_advertisement=date_of_advertisement,
                        closing_date=closing_date,
                        status=status,
                        area=area_name,
                        updated_date=self._today)

                    result = self.DB_connection.execute(on_duplicate_key_stmt)
                    _logger.info('added')
                except Exception as e:
                    _logger.error('Exception %s' % e)
                except db.exc.IntegrityError:  # grab the id of main post
                    _logger.info('Duplicate')

            time.sleep(2)
            # --- odds
            num_trs = 0
            for tr in range(first_idx, num_trs):
                try:
                    _tds = self.driver.find_elements_by_xpath(
                        "//tr[@class='ui-widget-content ui-datatable-odd']")[tr].find_elements_by_tag_name('td')
                    procurement_title = _tds[0].text
                    PTJ = _tds[1].text
                    date_of_advertisement = time.strptime(_tds[2].text, "%d/%m/%Y %H:%M %p")  # 05/10/2020 12:00 PM
                    closing_date = time.strptime(_tds[3].text, "%d/%m/%Y %H:%M %p")  # 05/10/2020 12:00 PM
                    ins = insert(self.table).values(
                        procurement_title=procurement_title,
                        PTJ=PTJ,
                        date_of_advertisement=date_of_advertisement,
                        closing_date=closing_date,
                        status=status,
                        area=area_name,
                        updated_date=self._today).prefix_with('IGNORE')
                    result = self.DB_connection.execute(ins)
                    print("added")
                except Exception as e:
                    _logger.error('Exception %s' % e)
                except db.exc.IntegrityError:  # grab the id of main post
                    _logger.info('Duplicate')

        except Exception as e:
            _logger.error('Exception %s' % e)

    def grab_whole(self, area_code, key, val, area_name):
        try:
            # # 'ADVERTISED' tab
            flag = True
            while flag:
                content_one_page = self.grab_content_one_page('advertised', area_code, key, val, area_name, first_idx=0)
                self.driver.find_element_by_tag_name('body').send_keys(Keys.PAGE_DOWN)
                time.sleep(2)
                try:  # next button
                    self.driver.find_element_by_xpath("//span[@class='ui-icon ui-icon-seek-next']").click()
                    time.sleep(5)
                except Exception as e:
                    # search button
                    self.driver.find_element_by_id('_scNoticeBoard_WAR_NGePportlet_:form:j_idt52').click()
                    time.sleep(2)

                    # Cancel button
                    self.driver.find_element_by_id('_scNoticeBoard_WAR_NGePportlet_:form:j_idt53:j_idt75').click()
                    time.sleep(2)
                    try:  # next button
                        self.driver.find_element_by_xpath(
                            "//span[@class='ui-paginator-next ui-state-default ui-corner-all']").click()
                        time.sleep(5)
                    except:
                        flag = False

                try:  # when next is disable
                    if self.driver.find_elements_by_xpath(
                            '//span[@class="ui-paginator-next ui-state-default ui-corner-all ui-state-disabled"]'):
                        flag = False
                except:
                    pass

            # select the 'CLOSED' tab
            self.driver.find_elements_by_xpath('//li[@role="tab"]')[2].click()
            time.sleep(10)

            # 'CLOSED' tab
            flag = True
            while flag:
                time.sleep(5)
                self.grab_content_one_page('closed', area_code, key, val, area_name, first_idx=10)
                self.driver.find_element_by_tag_name('body').send_keys(Keys.PAGE_DOWN)
                time.sleep(5)
                try:  # next button
                    self.driver.find_element_by_xpath(
                        "//span[@class='ui-paginator-next ui-state-default ui-corner-all']").click()
                    time.sleep(5)
                except Exception as e:
                    # search button
                    self.driver.find_element_by_id('_scNoticeBoard_WAR_NGePportlet_:form:j_idt52').click()
                    time.sleep(2)

                    # Cancel button
                    self.driver.find_element_by_id('_scNoticeBoard_WAR_NGePportlet_:form:j_idt53:j_idt75').click()
                    time.sleep(2)
                    try:  # next button
                        self.driver.find_element_by_xpath(
                            "//span[@class='ui-paginator-next ui-state-default ui-corner-all']").click()
                        time.sleep(5)
                    except:
                        flag = False

                try:  # when next is disable
                    if self.driver.find_elements_by_xpath(
                            '//span[@class="ui-paginator-next ui-state-default ui-corner-all ui-state-disabled"]'):
                        flag = False
                except:
                    flag = False
            return True

        except Exception as e:
            _logger.error('Exception %s' % e)

    def a_new_search(self, area_code, _items_sub1, _items_sub2, area_name):
        try:
            # search button
            self.driver.find_element_by_id('_scNoticeBoard_WAR_NGePportlet_:form:j_idt52').click()
            time.sleep(2)

            self.driver.find_element_by_id(
                '_scNoticeBoard_WAR_NGePportlet_:form:j_idt53:catCodeLvl1').find_elements_by_tag_name(
                "option")[area_code].click()
            time.sleep(2)

            self.driver.find_element_by_id(
                '_scNoticeBoard_WAR_NGePportlet_:form:j_idt53:catCodeLvl2').find_elements_by_tag_name(
                "option")[_items_sub1].click()
            time.sleep(2)

            for j in range(1, _items_sub2):
                try:
                    self.driver.find_element_by_id(
                        '_scNoticeBoard_WAR_NGePportlet_:form:j_idt53:catCodeLvl3').find_elements_by_tag_name(
                        "option")[j].click()
                    time.sleep(2)
                except:
                    continue

                # ok button
                self.driver.find_element_by_id('_scNoticeBoard_WAR_NGePportlet_:form:j_idt53:j_idt74').click()
                time.sleep(2)

                # second Ok
                self.driver.find_element_by_id('_scNoticeBoard_WAR_NGePportlet_:form:nbSearchButton').click()
                time.sleep(5)

                self.grab_whole(area_code, _items_sub1, _items_sub2, area_name)
            return True

        except Exception as e:
            _logger.error('Exception %s' % e)
            return False

    def collect_num_items(self, item_number):
        try:
            time.sleep(5)
            # search button
            self.driver.find_element_by_id('_scNoticeBoard_WAR_NGePportlet_:form:j_idt52').click()
            time.sleep(2)

            # level 1
            self.driver.find_element_by_id(
                '_scNoticeBoard_WAR_NGePportlet_:form:j_idt53:catCodeLvl1').find_elements_by_tag_name(
                "option")[item_number].click()
            time.sleep(2)

            # level 2
            _num_options_l2 = len(self.driver.find_element_by_id(
                '_scNoticeBoard_WAR_NGePportlet_:form:j_idt53:catCodeLvl2').find_elements_by_tag_name("option"))

            retuen_vals = {}
            for i in range(1, _num_options_l2):
                try:
                    _num_options_l2 = self.driver.find_element_by_id(
                        '_scNoticeBoard_WAR_NGePportlet_:form:j_idt53:catCodeLvl2').find_elements_by_tag_name(
                        "option")[i].click()
                    time.sleep(3)

                    _num_options_l3 = len(self.driver.find_element_by_id(
                        '_scNoticeBoard_WAR_NGePportlet_:form:j_idt53:catCodeLvl3').find_elements_by_tag_name("option"))
                    retuen_vals[i] = _num_options_l3
                except:
                    pass

            # Cancel button
            self.driver.find_element_by_id('_scNoticeBoard_WAR_NGePportlet_:form:j_idt53:j_idt75').click()
            time.sleep(2)

            return retuen_vals
        except Exception as e:
            _logger.error('Exception %s' % e)
            return {}

    def construction_projects(self):
        try:
            self.driver.get(self.config['construction_projects'])
            # self.driver.implicitly_wait(50)
            delay = 5  # seconds
            try:
                WebDriverWait(self.driver, delay).until(EC.presence_of_element_located(
                    (By.ID, '_scNoticeBoard_WAR_NGePportlet_:form:catCodeImg')))
                print("Page is ready!")
            except TimeoutException:
                _logger.error('Loading took too much time!')

            time.sleep(2)

            for area_code in self.config['area']:
                print(area_code)
                area_name = self.config['area'][area_code]

                dict_retuen_vals = self.collect_num_items(area_code)

                for key, val in dict_retuen_vals.items():
                    self.driver.refresh()
                    time.sleep(4)
                    self.a_new_search(area_code, key, val, area_name)
            return True
        except Exception as e:
            _logger.error('Exception %s' % e)
            return False

    def run(self):
        try:
            self.setup()
            if self.target_website == 'construction_projects':
                _res = self.construction_projects()
        except Exception as e:
            _logger.error('Exception %s' % e)
        finally:
            self.driver.quit()
