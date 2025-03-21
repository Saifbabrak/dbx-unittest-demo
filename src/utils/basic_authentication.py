import base64

def base64_authentication(username: str, password: str) -> str:

    userpass = username + ':' + password
    basic_token = base64.b64encode(userpass.encode()).decode()

    return f"Basic {basic_token}"

