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
python3 server.py
```



### Production

Start server instance with gunicorn
and bind the server with port 52428:

```
gunicorn server:app --bind localhost:52428
```

### Testing usage

```
import requests

url = "http://localhost:5000"
print(requests.get(url).json())

def cs_write(script):
    return requests.post(url + "/write", json={"script":script})

cs_write("Hello world!")
cs_write("import nest\n\nnest.ResetKernel()")
```

See the changes in script.py