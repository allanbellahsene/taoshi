import secrets
import string

def generate_api_key(length=32):
    """
    Generate a secure API key.

    Args:
        length (int): The length of the API key. Default is 32 characters.

    Returns:
        str: A securely generated API key.
    """
    alphabet = string.ascii_letters + string.digits
    api_key = ''.join(secrets.choice(alphabet) for _ in range(length))
    return api_key

# Usage
new_api_key = generate_api_key()
print(f"Generated API Key: {new_api_key}")
