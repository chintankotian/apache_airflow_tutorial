import requests

api_key = "b1M1nFjmH2yLXMwaUj5pxwp2XqTW3wnA"
city = "bengaluru"
url = f"https://api.tomorrow.io/v4/weather/realtime?location={city}&apikey={api_key}"


headers = {
    "accept": "application/json",
    "accept-encoding": "deflate, gzip, br"
}

try:
    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Raise an error for bad responses (4xx and 5xx)
    data = response.json()
    print(data)
except requests.exceptions.RequestException as e:
    print(f"An error occurred: {e}")
