import os
import requests
mailgun_api_key = os.getenv('MAILGUN_API_KEY')

def send_email(to_address, subject, body):

    mailgun_url = f"https://api.mailgun.net/v3/ourresearch.org/messages"

    mailgun_auth = ("api", mailgun_api_key)

    mailgun_data = {
        "from": "OurResearch Mailgun <mailgun@ourresearch.org>",
        "to": [to_address],
        "subject": subject,
        "text": body,
    }

    print(f"sending email to {to_address}")
    requests.post(mailgun_url, auth=mailgun_auth, data=mailgun_data)
    print("sent email")

if __name__ == "__main__":
    to_address = "dev@ourresearch.org"
    subject = "Test Email"
    body = "This is a test of the Mailgun API. Please discard this message"
    send_email(to_address, subject, body)