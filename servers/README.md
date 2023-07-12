### Co-Sim Server



#### Requirements

- Flask
- Gunicorn?

```
python3 -m pip install flask gunicorn
```


### Development / Testing

Start development server instance with flask on port 5000:

```
python3 app_server.py
```



### Production

Start server instance with gunicorn
and bind the server with port 52428:

```
gunicorn app_server:app --bind localhost:52428
```

### Testing usage

```
import requests

url = "http://localhost:5000"
print(requests.get(url).json())

def submit(script):
    return requests.post(url + "/submit", json={"script":script})

submit("Hello world!")
submit("import nest\n\nnest.ResetKernel()")
```

See the changes in script.py