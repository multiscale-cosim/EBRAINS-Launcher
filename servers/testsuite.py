import requests

url = "http://localhost:5000"
print(requests.get(url).json())

def submit(script):
    return requests.post(url + "/submit", json={"script":script})

submit("Hello world!")
submit("import nest\n\nnest.ResetKernel()")
