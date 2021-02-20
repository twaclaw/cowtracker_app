import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Dict


class Email():
    def __init__(self, conf: Dict[str, Any]):
        self.user = conf['user']
        self.password = conf['password']
        self.to = conf['to']
        self.smtp_server = conf['smtp_server']
        self.port = conf['port']

    def send_email(self, subject: str, content: str):
        msg = MIMEMultipart()
        msg['Subject'] = subject
        msg['From'] = self.user
        msg['To'] = ','.join(self.to)
        msg.attach(MIMEText(content, "plain"))

        text = msg.as_string()

        context = ssl.create_default_context()
        with smtplib.SMTP(self.smtp_server, self.port) as server:
            server.starttls(context=context)
            server.login(self.user, self.password)
            server.sendmail(self.user, self.to, text)
