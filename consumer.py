#! /usr/bin/python3
from kafka import KafkaConsumer
import json
import smtplib

gmailSend = 'IlanEmailSend@gmail.com'
gmailSendPassword = 'IlanEmailSend12'

gmailReceive = 'IlanEmailRecive@gmail.com'
gmailReceivePassword = 'IlanEmailRecive12'


def tweetsConsumer():
    consumer = KafkaConsumer('tweets')
    for msg in consumer:
        print("Record caught by consumer...")
        emailSender(msg)

def emailSender(text):
    header = "Twitter Consumer"
    emailText = ("From: %s \nTo: %s \nSubject: %s \n\n%s" %(gmailSend, gmailReceive, header, text.value))

    try:
        smtp_server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        smtp_server.ehlo()
        smtp_server.login(gmailSend, gmailSendPassword)
        smtp_server.sendmail(gmailSend, gmailReceive, emailText)
        smtp_server.close()
        print ("Email sent successfully!")
        print("Data is: " + str(text.value) + "\n\n")
    except Exception as ex:
        print ("Something went wrong….",ex)

def main():
    print("Starting consumer...\n")
    print("Using: ", gmailSend, " as Sender")
    print("Using: ", gmailReceive, " as Receiver\n")
    tweetsConsumer()


if __name__ == "__main__":
    main()

