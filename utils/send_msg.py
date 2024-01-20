import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
import base64


class MailSender(object):

    def __init__(self, smtp_server='smtp.qq.com', smtp_port=465, password='fcxuqasfweigecai',
                 from_mail='2394023336@qq.com'):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.password = password
        self.from_mail = from_mail
        # 设置发送人，必须base64编码，不然报错
        fromname64 = base64.b64encode(bytes("sender", 'utf8'))
        fromname64 = str(fromname64, 'utf-8')
        self.fromname64 = '"=?utf-8?B?' + fromname64 + '=?=" <' + self.from_mail + ">"
        self.__login()

    def __login(self):
        self.smtpObj = smtplib.SMTP_SSL('smtp.qq.com', 465)
        self.smtpObj.login(self.from_mail, self.password)

    def send_html_data(self, to_mail, cc_mail, subject, html_msg):
        message = MIMEMultipart()
        html_msg = MIMEText(html_msg, 'html', 'utf-8')
        message.attach(html_msg)
        message['From'] = Header(self.fromname64)
        if to_mail is not None and isinstance(to_mail, list):
            message['To'] = Header(",".join(to_mail), 'utf-8')
        if cc_mail is not None and isinstance(cc_mail, list):
            message['Cc'] = Header(",".join(cc_mail), 'utf-8')
        message['Subject'] = Header(subject, 'utf-8')
        self.smtpObj.sendmail(self.from_mail,to_mail, message.as_string())

    def close(self):
        self.smtpObj.quit()
