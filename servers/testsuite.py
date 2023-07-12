import requests

url = "http://localhost:5000"
print(requests.get(url).json())

def cs_write(script):
    return requests.post(url + "/write", json={"script":script})

cs_write("Hello world!")
cs_write("import nest\n\nnest.ResetKernel()")
