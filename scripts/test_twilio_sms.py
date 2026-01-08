from app.messaging import alert_config_from_env, send_sms, require_twilio
from dotenv import load_dotenv

load_dotenv()

cfg = alert_config_from_env()
twilio = require_twilio(cfg)

send_sms(
    account_sid=twilio.account_sid,
    auth_token=twilio.auth_token,
    from_number=twilio.from_number,
    to_number="+17326065457",
    body="✅ Test message from NCAA Halftime Predictor"
)