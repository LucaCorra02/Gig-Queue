import requests
from utils import (run_tests, new_event, new_user, buy_id, wait_for_outcomes, wait_until)


MAILPIT_API = "http://localhost:8025/api/v1"
USER_DOMAIN = "gig-queue.test"
ADMIN_EMAIL = "security@gig-queue.test"

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

TESTS = [
    test_confirmed_order_email,
]

def clear_inbox():
    requests.delete(f"{MAILPIT_API}/messages", timeout=10)

if __name__ == "__main__":
    clear_inbox()
    run_tests(TESTS)