import requests

API_URL = "http://localhost:8080"

def test_buy_ticket_success():
    payload = {"event_id": "live-verdena", "user_id": "user_01"}
    response = requests.post(f"{API_URL}/buy", json=payload)

    assert response.status_code == 202, response.text
    data = response.json()
    assert data["order_id"]
    assert data["status"] == "queued"
    assert isinstance(data["partition"], int)
    assert isinstance(data["offset"], int)

def test_buy_ticket_missing_event():
    payload = {"user_id": "user_01"}
    response = requests.post(f"{API_URL}/buy", json=payload)
    assert response.status_code == 422

def test_buy_ticket_empty_user():
    payload = {"event_id": "live-verdena", "user_id": "   "}
    response = requests.post(f"{API_URL}/buy", json=payload)
    assert response.status_code == 422

if __name__ == "__main__":
    test_buy_ticket_success()
    test_buy_ticket_missing_event()
    test_buy_ticket_empty_user()
    print("All tests passed")
