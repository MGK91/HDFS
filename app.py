from flask import Flask, render_template, redirect, url_for
from flask_bootstrap import Bootstrap
from flask import request
from flask_wtf import FlaskForm 
from wtforms import StringField, PasswordField, BooleanField
from wtforms.validators import InputRequired, Email, Length
from flask_sqlalchemy  import SQLAlchemy
from werkzeug.security import generate_password_hash, check_password_hash
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
import io
import pandas as pd
import random
import pickle
import requests
from flask import Response
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure
from matplotlib import pyplot
import datetime as dt
import numpy as np
from pretty_html_table import build_table
import subprocess
import sys
import os
import json
app = Flask(__name__)
app.config['SECRET_KEY'] = 'Thisissupposedtobesecret!'
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:////tmp/database.db'
bootstrap = Bootstrap(app)
db = SQLAlchemy(app)
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'
with open('/tmp/test.json', 'r') as f:
    data = json.load(f)
total = 0
value_dict = {}
value_dict1 = {}
for window in data['windows']:
    windows_length = len(window)
    for opstype in window['ops']:
        ops_length = len(opstype)
        for opstype1 in opstype['opType'].split():
            window_value = window.get('windowLenMs')
            total = total+window_value
            value_dict[opstype1] = window.get('windowLenMs')
max_key = max(value_dict, key=value_dict.get)
res = {k: np.percentile(v, 90) for k,v in value_dict.items()}
res1 = {k: np.percentile(v, 95) for k, v in value_dict.items()}
df = pd.read_csv("/opt/hadoop/hadoop-3.3.0/logs/hdfs-audit.log", skiprows=1, names=["Date", "Time", "LOGLEVEL", "system",  "permission",  "user",  "Authentication",  "IP", "Operation", "Source", "Destination", "Permission", "Protocol"], delim_whitespace=True)
df["Time"] = pd.to_datetime(df["Time"])
df1 = df
last_hour_time = dt.datetime.now() - dt.timedelta(hours =3)
last_time = last_hour_time.strftime('%H:%M:%S,%f')
last_nhour = last_time[:-3]
new_data = df[df["Time"] > last_nhour]
load_data = new_data[['user', 'Operation', 'Source', 'Destination']].sort_values(by='Destination', ascending=False)
user_data = new_data[['user', 'Operation']]
user_data1 = user_data[["user", "Operation"]].groupby(["user"]).count().sort_values("Operation", ascending=False)
#high_user_op = build_table(user_data1, 'blue_light')
high_user_op = user_data1.to_html()
dir_data = new_data[['user', 'Operation', 'Destination']]
new_dir = dir_data[["Destination", 'Operation']].groupby(["Destination"]).count().sort_values("Operation", ascending=False).head(3)
#dir_html = build_table(new_dir, 'blue_light')
dir_html = new_dir.to_html()
#load_data.to_html('/tmp/finish/template/LoadGenerated.html')
load_html = build_table(load_data, 'blue_light')
user_class_data = new_data[['user', 'Operation']]
user_classfiication = user_class_data.groupby(["Operation"]).agg(["count"])
user_class_html = user_classfiication.to_html()
####FileOperations
#updated_file_read = open("/tmp/test6.txt", "r")
#UPDATED_FILE = updated_file_read.read()
#block_file_read = open("/tmp/test3.txt", "r")
#BLOCK_FILE = block_file_read.read()
#print("Location details of Last Generated file")
#location_file_read = open("/tmp/test.txt", "r")
#LOCATION_FILE = location_file_read.read()
#print(LOCATION_FILE)
#print("Rack and Data node details of Last Generated file")
#rack_file_read = open("HDFS_RACK.txt", "r")
#RACK_FILE = rack_file_read.read()
#print(RACK_FILE)
#rack_file_read = open("/tmp/test1.txt", "r")
#RACK_FILE = rack_file_read.read()
#print("Newest file in the test directory ")
#old_file_read = open("/tmp/test4.txt", "r")
#NEW_FILE = old_file_read.read()
#print(NEW_FILE)
#print("Oldest file in test directory")
#newfile_read = open("/tmp/test5.txt", "r")
#OLD_FILE = newfile_read.read()
#print(OLD_FILE)
class User(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(15), unique=True)
    email = db.Column(db.String(50), unique=True)
    password = db.Column(db.String(80))

@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))

class LoginForm(FlaskForm):
    username = StringField('username', validators=[InputRequired(), Length(min=4, max=15)])
    password = PasswordField('password', validators=[InputRequired(), Length(min=8, max=80)])
    remember = BooleanField('remember me')

class RegisterForm(FlaskForm):
    email = StringField('email', validators=[InputRequired(), Email(message='Invalid email'), Length(max=50)])
    username = StringField('username', validators=[InputRequired(), Length(min=4, max=15)])
    password = PasswordField('password', validators=[InputRequired(), Length(min=8, max=80)])


@app.route('/')
def index():
    return render_template('index.html')
@app.route('/formstatic', methods=['GET', 'POST'])
def formstatic():
    return render_template('form.html')
@app.route('/login', methods=['GET', 'POST'])
def login():
    form = LoginForm()

    if form.validate_on_submit():
        user = User.query.filter_by(username=form.username.data).first()
        if user:
            if check_password_hash(user.password, form.password.data):
                login_user(user, remember=form.remember.data)
                return redirect(url_for('dashboard'))

        return '<h1>Invalid username or password</h1>'
        #return '<h1>' + form.username.data + ' ' + form.password.data + '</h1>'

    return render_template('login.html', form=form)

@app.route('/NameNodeHealth')
@login_required
def NameNodeHealth():
    #fig = create_figure()
    #output = io.BytesIO()
    #FigureCanvas(fig).print_png(output)
    file_health = open("file_health.txt", "w")
    command_file_health = "/opt/hadoop/hadoop-3.3.0/bin/hdfs dfsadmin -report|tee HDFS_NN_REPORT.txt"
    value_file_health = subprocess.call(command_file_health, shell=True, stdout=file_health)
    nn_read_file = open("file_health.txt", "r")
    NNHealth = nn_read_file.read()
    return render_template('NameNodeHealth.html', NNHealth=NNHealth)

def create_figure():
    fig = Figure()
    an = pd.read_excel('Test_Data.xlsx', 'Sheet1')
    #Basic plot
    pyplot.plot(an['Applications'],an['Priority'], 'bo', an['Applications'],an['Priority'], 'k')
    pyplot.xlabel('Application')
    pyplot.ylabel('priority')
    pyplot.title('Graph')
    pyplot.xticks(rotation=45)
    pyplot.legend()
    return fig

@app.route('/signup', methods=['GET', 'POST'])
def signup():
    form = RegisterForm()

    if form.validate_on_submit():
        hashed_password = generate_password_hash(form.password.data, method='sha256')
        new_user = User(username=form.username.data, email=form.email.data, password=hashed_password)
        db.session.add(new_user)
        db.session.commit()

        return '<h1>New user has been created!</h1>'
        #return '<h1>' + form.username.data + ' ' + form.email.data + ' ' + form.password.data + '</h1>'

    return render_template('signup.html', form=form)

@app.route('/dashboard')
@login_required
def dashboard():
    return render_template('dashboard.html', name=current_user.username)
@app.route('/LoadGenerated')
@login_required
def LoadGenerated():
    #load_data = new_data[['user', 'Operation', 'Source', 'Destination']].sort_values(by='Destination', ascending=False)
    #load_data.to_html('/tmp/finish/template/LoadGenerated.html')
    return load_html 
@app.route('/HighUserOperation')
@login_required
def HighUserOperation():
    return high_user_op
@app.route('/DirectoryOperation')
@login_required
def DirectoryOperation():
    return dir_html
@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('index'))
@app.route('/UserClassfication')
@login_required
def UserClassification():
    return user_class_html 
@app.route('/Latency')
@login_required
def Latency():
    return render_template('Latency.html', name=current_user.username, max_key=max_key, res=res, res1=res1)
  
@app.route('/FileOperations')
@login_required
def FileOperations():
    exec(open('/tmp/fileoperations.py', 'r').read())
    updated_file_read = open("/tmp/test6.txt", "r")
    UPDATED_FILE = updated_file_read.read()
    block_file_read = open("/tmp/test3.txt", "r")
    BLOCK_FILE = block_file_read.read()
    print("Location details of Last Generated file")
    location_file_read = open("/tmp/test.txt", "r")
    LOCATION_FILE = location_file_read.read()
    #print(LOCATION_FILE)
    print("Rack and Data node details of Last Generated file")
    #rack_file_read = open("HDFS_RACK.txt", "r")
    #RACK_FILE = rack_file_read.read()
    #print(RACK_FILE)
    rack_file_read = open("/tmp/test1.txt", "r")
    RACK_FILE = rack_file_read.read()
    print("Newest file in the test directory ")
    old_file_read = open("/tmp/test4.txt", "r")
    NEW_FILE = old_file_read.read()
    #print(NEW_FILE)
    print("Oldest file in test directory")
    newfile_read = open("/tmp/test5.txt", "r")
    OLD_FILE = newfile_read.read()
    print(OLD_FILE)

    return render_template('FileOps.html', name=current_user.username, UPDATED_FILE=UPDATED_FILE, BLOCK_FILE=BLOCK_FILE, LOCATION_FILE=LOCATION_FILE, RACK_FILE=RACK_FILE, NEW_FILE=NEW_FILE, OLD_FILE=OLD_FILE)

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
