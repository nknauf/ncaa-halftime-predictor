import os
from twilio.rest import Client

def main():
    sid = os.environ["TWILIO_ACCOUNT_SID"]
    token = os.environ["TWILIO_AUTH_TOKEN"]
    from_num = os.environ["TWILIO_FROM_NUMBER"]

    to_num = input("Enter your phone number, ex.(+1...): ").strip()
    client = Client(sid, token)

    msg = client.messages.create(
        from_=from_num,
        to=to_num,
        body="Twilio test: your SMS pipeline is working ✅",
    )
    print("Sent. SID:", msg.sid)

if __name__ == "__main__":
    main()
