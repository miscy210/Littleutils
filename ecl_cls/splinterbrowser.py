# -*- coding: utf8 -*-
'''
Created on 2017-04-14

@author: miscy210 <miscy210@163.com>
'''
from splinter import Browser
from splinter.driver.webdriver.chrome import Options, Chrome

class SplinterBrowser():
    def __init__(self, webdriver='chrome', proxy_info=None):
        self.proxy_info = proxy_info
        self.webdriver = webdriver
        if self.proxy_info:
            if webdriver == 'firefox':
                self.browser = Browser(webdriver, profile_preferences=self.setproxy())
            elif webdriver == 'chrome':
                self.browser = Browser(webdriver)
                self.browser.driver = Chrome(chrome_options=self.setproxy())
        else:
            self.browser = Browser(webdriver)

    def setproxy(self):
        if self.proxy_info:
            [host, port] = self.proxy_info.split(":")
            if self.webdriver == 'firefox':
                proxy_settings = {'network.proxy.type': 1,
                                  'network.proxy.http': host,
                                  'network.proxy.http_port': int(port),
                                  'network.proxy.ssl': host,
                                  'network.proxy.ssl_port': int(port),
                                  'network.proxy.socks': host,
                                  'network.proxy.socks_port': int(port),
                                  'network.proxy.ftp': host,
                                  'network.proxy.ftp_port': int(port)
                                  }
                return proxy_settings
            elif self.webdriver == 'chrome':
                chrome_options = Options()
                chrome_options.add_argument('--proxy-server=http://%s' % self.proxy_info)
                return chrome_options
        else:
            return None

    def quit(self):
        self.browser.quit()