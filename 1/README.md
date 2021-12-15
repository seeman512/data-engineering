## Install packages under virtual environment
```
pip3 install -r requirements.txt
```

## Setup environment variables
Create file **.env** with username, password
```
export app_username=$app_username
export app_password=$app_password
```
and run `source .env`
or set username and password in **config.yaml**

## Launch script
```
python3 app.py
```
