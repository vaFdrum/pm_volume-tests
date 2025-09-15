"""Authentication helpers"""

from urllib.parse import urljoin

from bs4 import BeautifulSoup

from config import CONFIG


def extract_login_form(html, username, password):
    """Extract login form data from HTML"""
    soup = BeautifulSoup(html, features="html.parser")
    form = soup.find("form")
    if not form or not form.get("action"):
        return None
    action_url = urljoin(CONFIG["api"]["base_url"], form["action"])
    return {
        "action": action_url,
        "payload": {
            "flowType": "byLogin",
            "username": username,
            "formattedUsername": username,
            "password": password,
        },
    }
