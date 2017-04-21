#!/usr/bin/python3

import MySQLdb
import syslog
import time
from configparser import ConfigParser
from urllib.request import urlopen
from xml.etree import ElementTree as et
from xml.dom.minidom import parse

parser = ConfigParser()
parser.read('/home/sysad/MEGA/tmp/ot/fd.conf')
db_con = {}
items = parser.items('mysql')
for item in items:
    db_con[item[0]] = item[1]

#
# cursor = db.cursor()
# sql = "INSERT INTO cd VALUES ('Empire Burlesque', 'Bob Dylan', '10.90', '1985');"
# cursor.execute(sql)
# db.commit()
# db.close()

urlmy = parser.sections()[0]
delay = int(parser[urlmy]['delay'])


class ApptParser:
    def __init__(self, url):
        self.url = url
        self.str_data = ''
        self.xml_data = ''
        self.db = ''

    def try_url(self):
        try:
            usock = urlopen(self.url)
            self.str_data = usock.read()

        except:
            syslog.syslog("[OMG, it's a ERR] Name or service {} not known".format(self.url))
        else:
            syslog.syslog("Connection with {} successful established".format(self.url))

    def try_parse(self):
        if self.str_data:
            try:
                self.xml_data = et.fromstring(self.str_data)
                syslog.syslog('XML well-formed')
            except:
                syslog.syslog("[OMG, it's a ERR] Sorry, not well-formed XML")

    def try_mysql_conection(self, db_con):
        try:
            self.db = MySQLdb.connect(**db_con)
        except:
            syslog.syslog("[OMG, it's a ERR] Connection with database failed")
        else:
            syslog.syslog("Connection with database successful established")

    def db_feed(self):
        if self.db:
            cursor = self.db.cursor()
            cd_list = self.xml_data.findall('CD')
            for x in cd_list:
                TITLE = x.find('TITLE').text.replace("'","")
                ARTIST = x.find('ARTIST').text.replace("'","")
                PRICE = x.find('PRICE').text
                YEAR = x.find('YEAR').text
                sql = "INSERT INTO cdcatalog VALUES ('{}', '{}', '{}', '{}');".format(
                    TITLE,
                    ARTIST,
                    PRICE,
                    YEAR
                )
                cursor.execute(sql)
                self.db.commit()
            self.db.close()
            syslog.syslog("DB feed SUCCESS!!!")


if __name__ == '__main__':
    while True:
        syslog.openlog('testDaemon', 0, syslog.LOG_LOCAL4)
        my = ApptParser(urlmy)
        my.try_url()
        my.try_parse()
        my.try_mysql_conection(db_con)
        my.db_feed()
        for i in range(delay):
            syslog.syslog('Delay on {} sec'.format(delay - i))
            i -= 1
            time.sleep(1)
        syslog.closelog()
