import requests
import redis
from utils import (run_tests, new_event, new_user, buy_id, wait_for_outcomes, wait_until, post_buy
                   , USER_THRESHOLD, new_ip, exist_blocked_user)

MAILPIT_API = "http://localhost:8025/api/v1"
USER_DOMAIN = "gig-queue.test"
ADMIN_EMAIL = "security@gig-queue.test"
REDIS_URL = "redis://localhost:6379/0"
rdb = redis.Redis.from_url(REDIS_URL, decode_responses=True)

def inbox(limit=200):
    r = requests.get(f"{MAILPIT_API}/messages", params={"limit": limit}, timeout=10)
    r.raise_for_status()
    return r.json()["messages"]

def messages_to(address, limit=200):
    found_messages = []
    all_messages = inbox(limit)
    for message in all_messages:
        for recipient in message["To"]:
            if recipient["Address"] == address:
                found_messages.append(message)
                break
    return found_messages

def wait_for_mail(address, count=1, timeout=40):
    found = []
    def check():
        nonlocal found
        found = messages_to(address)
        return len(found) >= count
    assert wait_until(check, timeout), f"expected {count} email(s) to {address}, got {len(found)}"
    return found

def body_of(message_id):
    r = requests.get(f"{MAILPIT_API}/message/{message_id}", timeout=10)
    r.raise_for_status()
    return r.json().get("Text", "")

def test_confirmed_order_email():
    event, user = new_event(seats=20), new_user()
    order_id = buy_id(event, user_id=user, quantity=3)
    wait_for_outcomes([order_id])

    mails = wait_for_mail(f"{user}@{USER_DOMAIN}")
    assert len(mails) == 1, f"expected 1 email, got {len(mails)}"
    mail = mails[0]
    assert "confirmed" in mail["Subject"].lower(), mail["Subject"]
    assert event in mail["Subject"], mail["Subject"]

    body = body_of(mail["ID"])
    assert order_id in body
    assert "1-3" in body, f"seat range missing from body: {body}"

def test_rejected_order_email():
    event = new_event(seats=1)
    buy_id(event)
    user = new_user()
    order_id = buy_id(event, user_id=user)
    wait_for_outcomes([order_id])

    mail = wait_for_mail(f"{user}@{USER_DOMAIN}")[0]
    assert "rejected" in mail["Subject"].lower()
    body = body_of(mail["ID"])
    assert "sold out" in body.lower(), body

def test_fraud_alert_email():
    before = len(messages_to(ADMIN_EMAIL))
    event, user, ip = new_event(seats=100), new_user(), new_ip()

    for _ in range(USER_THRESHOLD + 5):
        post_buy(event_id=event, user_id=user, ip=ip)
    assert wait_until(lambda: exist_blocked_user(user)), "user never blocked"

    def admin_got_it():
        return any(user in m["Subject"] for m in messages_to(ADMIN_EMAIL))
    assert wait_until(admin_got_it, timeout=40), "no security alert reached the admin"

    mail = next(m for m in messages_to(ADMIN_EMAIL) if user in m["Subject"])
    assert "SECURITY ALERT" in mail["Subject"]
    body = body_of(mail["ID"])
    assert ip in body, body
    assert "user_rate" in body, body
    assert len(messages_to(ADMIN_EMAIL)) > before, "admin inbox did not grow"

def test_notification_counters():
    before = int(rdb.get("notifications:count") or 0)
    event, user = new_event(seats=10), new_user()
    order_id = buy_id(event, user_id=user)
    wait_for_outcomes([order_id])
    wait_for_mail(f"{user}@{USER_DOMAIN}")
    
    assert wait_until(lambda: int(rdb.get("notifications:count") or 0) > before), "notifications count not grew"
    assert rdb.llen("notifications:recent") <= 50, "recent list is not bounded"

TESTS = [
    test_confirmed_order_email,
    test_rejected_order_email,
    test_fraud_alert_email,
    test_notification_counters,
]

def clear_inbox():
    requests.delete(f"{MAILPIT_API}/messages", timeout=10)

if __name__ == "__main__":
    clear_inbox()
    run_tests(TESTS)