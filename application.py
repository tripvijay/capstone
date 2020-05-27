import pandas as pd
import requests as rs
from zipfile import ZipFile
from urllib.request import urlretrieve
import subprocess
import os
import glob
from pathlib import Path
import requests
import sys
import time
import datetime
from datetime import timedelta

import flask
import dash
import dash_html_components as html
import dash_core_components as dcc

from dash.dependencies import Input, Output
from flask_caching import Cache
from nocache import nocache
from m1_face_detect import m1_face_detect
from azure.cognitiveservices.vision.face import FaceClient
from msrest.authentication import CognitiveServicesCredentials
from io import BytesIO



external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']



app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

# Loading screen CSS
app.css.append_css({"external_url": "https://codepen.io/chriddyp/pen/brPBPO.css"})

application = app.server
app.title = 'Milestone app on AWS EB'

app.config.suppress_callback_exceptions = True
#app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0

#cache = Cache(app.server, config={
#    'CACHE_TYPE': 'filesystem',
#    'CACHE_DIR': 'cache'
#})

#timeout = 5

iDiff = 0
iCounter = 0

def generate_layout():

    return html.Div([
    html.H3 ('BUAD625 - Online Milestone verifications site - Vijay Tripasuri'),
    dcc.Tabs(id='tabs-example', value='tab-1', children=[
        dcc.Tab(label='Milestone 2', value='tab-1',style={'backgroundColor':'silver'}),
        dcc.Tab(label='Milestone 7', value='tab-2',style={'backgroundColor':'silver'}),
    ]),
    html.Div(id='tabs-example-content')
])

app.layout = generate_layout

@app.callback(Output('tabs-example-content', 'children'),
              [Input('tabs-example', 'value')])
def render_content(tab):
    str1 = "([html.H3('Fraudster detect'), html.H5('Step 1: Provide Milestone 2 input file URL and hit Submit button', style={'textAlign': 'left','color': 'blue'}),html.Div(dcc.Input(id='input-box', type='url', size='50')),html.Button('Submit', id='button1'),html.H5('Step 2: Download Milestone 2 result file using link below', style={'textAlign': 'left','color': 'blue'}),"
    str2 = "html.A('Download CSV', id=\"download_link\", href=\"/download_excel/\"),"
    str3 = "html.Div(id='output-container-button',children='Click on link to download file')])"
    str4 = "html.Div('Link to download Excel will be displayed here', id='static_text')"
    iCounter = 0

    if tab == 'tab-1':
        #iCounter = n_clicks
        if iCounter == 0:
            return html.Div([
                html.H3('Fraudster detect'),
                # html.H5('Vijay Tripasuri'),
                html.H5('Step 1: Provide Milestone 2 input file URL and hit Submit button', style={
                    'textAlign': 'left',
                    'color': 'blue'
                }),
                html.Div(dcc.Input(id='input-box', type='url', size='50')),
                html.Button('Submit', id='button1'),
                html.H5('Step 2: Download Milestone 2 result file using link generated', style={
                    'textAlign': 'left',
                    'color': 'blue'
                }),
                html.Div('Link to download Excel/CSV file will be displayed below', id='static_text'),
                html.Div(id='output-container-button',
                         children='Click on link to download file')
            ])
        else:
            #iCounter = iCounter + 1
            return html.Div([
                html.H3('Fraudster detect'),
                # html.H5('Vijay Tripasuri'),
                html.H5('Step 1: Provide Milestone 2 input file URL and hit Submit button', style={
                    'textAlign': 'left',
                    'color': 'blue'
                }),
                html.Div(dcc.Input(id='input-box', type='url', size='50')),
                html.Button('Submit', id='button1'),
                html.H5('Step 2: Download Milestone 2 result file using link generated', style={
                    'textAlign': 'left',
                    'color': 'blue'
                }),
                #html.A('Download CSV', id="download_link", href="/download_excel/"),
                html.Div(id='output-container-button',
                         children='Click on link to download file')
            ])
    elif tab == 'tab-2':
        return html.Div([
            html.H3('Online workflow'),
            # html.H5('Vijay Tripasuri'),
            html.H5('Step 1: Provide Milestone 7 input file URL and hit Submit button', style={
                'textAlign': 'left',
                'color': 'blue'
            }),
            html.Div(dcc.Input(id='input-box-2', type='url', size='50')),
            html.Button('Submit', id='button2'),
            html.H5('Step 2: Download Milestone 7 result file using link generated', style={
                'textAlign': 'left',
                'color': 'blue'
            }),
            html.Div('Link to download Excel/CSV file will be displayed below', id='static_text'),
            html.Div(id='output-container-button-2',
                     children='Click on link to download file')
        ])

@app.callback(
    dash.dependencies.Output('output-container-button', 'children'),
    [dash.dependencies.Input('button1', 'n_clicks')],
    [dash.dependencies.State('input-box', 'value')]
    )

def update_output(n_clicks, value):

    localtime_begin = datetime.datetime.now()
    #result = time.strftime("%I:%M:%S %p", localtime_begin)
    #print("Begin {}".format(result))

    temp_str = str(value)
    milestone_url = temp_str
    milestone_url_string = temp_str
    file_id = ""
    iCount = 0
    i = 0
    #diff = 0
    iDiff = 0
    seconds_in_day = 24 * 60 * 60


    #dropBoxM2URL = "https://www.dropbox.com/s/j22vzmkct7zccyj/4272368.zip?dl=1"
    #sample url = "https://www.dropbox.com/s/7ggakh21uyivqgr/sampleLoginAttemptsFile.zip?dl=1"

    # assign values to corresponding variables for folders and files
    #directory_fraudtest_to_extract_to = "extract"
    directory_fraudtest_to_extract_to = ""
    directory_output = "data_files"

    localM2FileName = "M2.zip"
    localFraudInputFileName = "sampleFraudTestInput.zip"
    localLiveCustomerListFileName = "liveCustomerList.csv"
    localFraudListFileName = "liveFraudList.csv"
    localFinalOutputFile = ""

    milestone_url = len(milestone_url)
    #time.sleep(2)



    # using the Popen function to execute the command and store the result in temp.
    # it returns a tuple that contains the data and the error if any.

    #temp = subprocess.Popen([cmdRM, args], stdout=subprocess.PIPE)

    try:
        if (milestone_url > 5):
            for f in glob.glob('*.jpg'):
                try:
                    os.remove(f)
                    #f.unlink()
                except OSError as e:
                    print("Error: %s : %s" % (f, e.strerror))

            for f in glob.glob('*.csv'):
                try:
                    if (f != 'liveCustomerList.csv' and f != 'liveFraudList.csv'
                            and f != 'liveBankAcct.csv'
                            and f != 'bankTransactions.csv'
                            and f != 'startBalance.csv'):
                        os.remove(f)
                except OSError as e:
                    print("Error: %s : %s" % (f, e.strerror))

            middle_str = temp_str.split('.', 4)[2]
            file_id = middle_str.split('/', 4)[3]

            localFinalOutputFile = file_id + ".csv"

            milestone_url_string = temp_str
            urlretrieve(milestone_url_string, localFraudInputFileName)

            ## unzip contents into corresponding folder
            with ZipFile(localFraudInputFileName) as zipObj:
                zipObj.extractall(directory_fraudtest_to_extract_to)

            # write content from res object on to a dataframe
            dfFraudInput = pd.DataFrame(columns=['cust_id', 'image_name'])

            #os.chdir(directory_fraudtest_to_extract_to)
            # os.chdir(directory_to_extract_to)

            for file in glob.glob("*.jpg"):
                dfFraudInput.loc[i,'cust_id'] = file[:4]
                dfFraudInput.loc[i, 'image_name'] = file
                i = i + 1



            iCount = dfFraudInput['cust_id'].count()


            # import data from Live Customer list csv on to a dataframe
            dfLiveCustomers = pd.read_csv(localLiveCustomerListFileName,
                                          dtype={'custID': str, 'firstName': str, 'lastName': str})

            dataTypeSeriesFraud = dfFraudInput.dtypes
            dataTypeSeriesLive = dfLiveCustomers.dtypes


            dataTypeSeriesFraud = dfFraudInput.dtypes
            dataTypeSeriesLive = dataTypeSeriesLive.dtypes


            # perform left outer join between Fraud Input and Live Customer dataframes
            dfResultLiveFraudCustomer = pd.merge(dfFraudInput, dfLiveCustomers, left_on='cust_id', right_on='custID',
                                                 how='left')

            # import data from Fraud list csv on to a dataframe
            dfFraudList = pd.read_csv(localFraudListFileName, dtype={'firstName': str, 'lastName': str})

            # converting case of first and last names in both dataframes to Upper

            dfFraudList['firstName'] = dfFraudList['firstName'].str.upper()
            dfFraudList['lastName'] = dfFraudList['lastName'].str.upper()

            dfResultLiveFraudCustomer['firstName'] = dfResultLiveFraudCustomer['firstName'].str.upper()
            dfResultLiveFraudCustomer['lastName'] = dfResultLiveFraudCustomer['lastName'].str.upper()

            dfResultFraudCustomerList = pd.merge(dfResultLiveFraudCustomer,
                                                 dfFraudList[['firstName', 'lastName']],
                                                 on=['firstName', 'lastName'],
                                                 how='left',
                                                 indicator=True)


            dfResultFraudCustomerList.loc[dfResultFraudCustomerList['_merge'] == "left_only", 'fraudster'] = '0'
            dfResultFraudCustomerList.loc[dfResultFraudCustomerList['_merge'] == "both", 'fraudster'] = '1'

            # print(dfResultFraudCustomerList.head(5))

            dfFinalResult = dfResultFraudCustomerList[['cust_id', 'fraudster']]
            # print(dfFinalResult.head(10))

            # print(directory_output+'/'+localFinalOutputFile)
            dfFinalResult.to_csv(localFinalOutputFile, index=False, header=True)

            localtime_end = datetime.datetime.now()

            diff = localtime_end - localtime_begin
            iDiff = diff.microseconds
            iDiff = iDiff/1000000
            #seconds_in_day = 24 * 60 * 60
            #divmod(difference.days * seconds_in_day + difference.seconds, 60)

        #return 'Time taken to execute in seconds is "{}". File name to download - {}'.format( iDiff,localFinalOutputFile)
        if (milestone_url > 5):
            return html.A('Download', id="download_link", href="/download_excel/")
        else:
            html.Div('Link to download Excel will be displayed here', id='static_text'),

    #except (RuntimeError, TypeError, NameError):
    except TypeError:
        return 'The URL entered is invalid "{}"'.format(
            value

    )



@app.server.route('/download_excel/')
@nocache
def download_excel():

    #cache = Cache(app.server, config={'CACHE_TYPE': 'filesystem'})
    #cache.clear()

    for f in glob.glob('*.csv'):
        try:
            if (f != 'liveCustomerList.csv' and f != 'liveFraudList.csv' and f != 'liveBankAcct.csv'
                            and f != 'bankTransactions.csv'
                            and f != 'startBalance.csv'):
                localFinalOutputFile = f
        except OSError as e:
            print("Error: %s : %s" % (f, e.strerror))

    #Return Excel
    return flask.send_file(localFinalOutputFile,
                           mimetype='text/csv',
                           attachment_filename=localFinalOutputFile,
                           as_attachment=True,
                           cache_timeout=0)

def isNaN(string):
    return string != string

@app.callback(
    dash.dependencies.Output('output-container-button-2', 'children'),
    [dash.dependencies.Input('button2', 'n_clicks')],
    [dash.dependencies.State('input-box-2', 'value')]
    )

def update_output_online7(n_clicks, value):
    localtime_begin = datetime.datetime.now()
    # result = time.strftime("%I:%M:%S %p", localtime_begin)
    # print("Begin {}".format(result))

    temp_str = str(value)
    milestone_url = temp_str
    milestone_url_string = temp_str
    file_id = ""
    iCount = 0
    iDiff = 0
    i = 0
    seconds_in_day = 24 * 60 * 60

    # assign values to corresponding variables for folders and files
    # directory_fraudtest_to_extract_to = "extract"
    directory_fraudtest_to_extract_to = ""
    directory_output = "data_files"
    directory_source = "data"

    localM2FileName = "M7.zip"
    localFraudInputFileName = "sampleFraudTestInput.zip"
    localLiveCustomerListFileName = "liveCustomerList.csv"
    localFraudListFileName = "liveFraudList.csv"
    localLiveBankAccountListFileName = "liveBankAcct.csv"
    startBalanceFile = "startBalance.csv"
    bankTransactionsFile = "bankTransactions.csv"

    localFinalOutputFile = ""

    milestone_url = len(milestone_url)

    # using the Popen function to execute the command and store the result in temp.
    # it returns a tuple that contains the data and the error if any.

    try:
        if (milestone_url > 5):
            for f in glob.glob('*.jpg'):
                try:
                    os.remove(f)
                    # f.unlink()
                except OSError as e:
                    print("Error: %s : %s" % (f, e.strerror))

            for f in glob.glob('*.csv'):
                try:
                    if (f != 'liveCustomerList.csv'
                            and f != 'liveFraudList.csv'
                            and f != 'liveBankAcct.csv'
                            and f != 'bankTransactions.csv'
                            and f != 'startBalance.csv'):
                        os.remove(f)
                except OSError as e:
                    print("Error: %s : %s" % (f, e.strerror))

            middle_str = temp_str.split('.', 4)[2]
            file_id = middle_str.split('/', 4)[3]

            localFinalOutputFile = file_id + ".csv"

            milestone_url_string = temp_str
            urlretrieve(milestone_url_string, localFraudInputFileName)

            ## unzip contents into corresponding folder
            with ZipFile(localFraudInputFileName) as zipObj:
                zipObj.extractall(directory_fraudtest_to_extract_to)

            # write content from res object on to a dataframe
            dfResultMilestone3 = pd.DataFrame(
                columns=['custID', 'bankAcctID', 'cust_image_name', 'match_ind', 'fraudster', 'rightAcctFlag'])
            # dfFraudFaceChecked = pd.DataFrame(columns=['cust_id', 'match_ind'])

            for file in glob.glob("*.jpg"):
                dfResultMilestone3.loc[i, 'custID'] = file[:4]
                dfResultMilestone3.loc[i, 'bankAcctID'] = file[5:11]
                dfResultMilestone3.loc[i, 'cust_image_name'] = file
                i = i + 1

            ########################################################################################################################
            ##MILESTONE 5 BLOCK - PREDICT NEXT PAY DATE
            ########################################################################################################################

            # import data from Live Customer list csv on to a dataframe
            dfStartBalance = pd.read_csv(startBalanceFile, dtype={'acctID': str, 'stDate': str, 'balAmt': float},
                                         parse_dates=True)

            dfTransactions = pd.read_csv(bankTransactionsFile, dtype={'tranDate': str, 'acctID': str, 'tranAmt': float},
                                         parse_dates=True)
            # train['C14'] = train.C14.astype(int)
            dfResultMilestone3['bankAcctID'] = dfResultMilestone3.bankAcctID.astype(int)

            dfFinalResult = dfResultMilestone3[
                ['custID', 'bankAcctID', 'match_ind', 'fraudster', 'rightAcctFlag', 'cust_image_name']]
            # dfFinalResult.to_csv(localFinalOutputFile, index=False, header=True)

            # dfFinalResult['bankAcctID'].astype(string).astype(int)

            dataTypeSeriesLeft = dfResultMilestone3.dtypes
            dataTypeSeriesRight = dfStartBalance.dtypes

            dfInputBankStBal = pd.merge(dfFinalResult,
                                        dfStartBalance[['bankAcctID', 'amt']],
                                        on=['bankAcctID'],
                                        how='left',
                                        indicator=True)
            # print(dfInputBankStBal)

            dfInputBankTrans = pd.merge(dfInputBankStBal,
                                        dfTransactions,
                                        on=['bankAcctID'],
                                        how='left',
                                        indicator='exists')

            dfSample1 = dfInputBankTrans.loc[dfInputBankTrans['transAmount'] >= 200]
            dfSample2 = dfSample1.loc[dfSample1['date'] >= '2020-01-01']

            # create empty dataset to be set values in the for loop
            dfFinalSet1 = pd.DataFrame(
                columns=['cust_id', 'acct_id', 'match_ind', 'fraudster', 'rightAcctFlag', 'inc_no', 'pay_date',
                         'weekday', 'transAmount', 'counter', 'next_pay_date',
                         'next_pay_flag', 'cust_image_name'])

            # convert date field to datetime format from string
            dfSample2['date'] = pd.to_datetime(dfSample2['date'], format="%Y-%m-%d")
            dfSample1['date'] = pd.to_datetime(dfSample1['date'], format="%Y-%m-%d")

            dfPayWithWeekDay = pd.DataFrame(columns=['acct_id', 'pay_date', 'weekday'])

            for i in range(0, len(dfSample1) - 1):
                dfPayWithWeekDay.loc[i, 'acct_id'] = dfSample1.iloc[i].bankAcctID
                dfPayWithWeekDay.loc[i, 'pay_date'] = dfSample1.iloc[i].date
                dfPayWithWeekDay.loc[i, 'weekday'] = dfSample1.iloc[i].date.weekday()
                # dfSample2.loc[i, 'weekday'] = "2" #

            dfSample3 = dfPayWithWeekDay.loc[dfPayWithWeekDay['acct_id'] == 278749]  # 220719]
            # dfSample3 = dfSample2.loc[dfSample2['bankAcctID'] == 212054]
            # print(dfSample3)

            dfSample4 = dfSample1.loc[dfSample1['bankAcctID'] == 278749]
            # print(dfSample4)
            # exit()

            # sort rows in dataframe based on Bank account ID and date of deposit
            dfSample2 = dfSample2.sort_values(by=['bankAcctID', 'date'])

            # initialize temp variables to 0 that will be used in the looping
            acct_id = 0
            days_increment = 0
            inc_counter = -1
            last_row_ind = 0
            first_pay_date = '2020-04-01'
            days_in_month = 28
            day_double = 2
            day_single = 1
            dfLengthCount = len(dfSample2) - 1

            # loop through each row in dataset created with bank account id, date of deposit of greater than $200 since Apr 1
            # use temp variables -
            #   acct_id to track first row within an account set
            #   days_increment to track gap in number of days between two deposit dates
            #   inc_counter to track last row within an account set for which next pay date will be calculated

            for i in range(0, len(dfSample2)):
                if (dfSample2.iloc[i].bankAcctID == acct_id):
                    last_row_ind = 0
                    days_increment = dfSample2.iloc[i].date - dfSample2.iloc[i - 1].date
                    if (i == len(dfSample2) - 1):
                        inc_counter = 1
                        last_row_ind = 1
                    else:
                        if (dfSample2.iloc[i].bankAcctID != dfSample2.iloc[i + 1].bankAcctID):
                            inc_counter = 1
                            last_row_ind = 1
                else:
                    days_increment = 0
                    inc_counter = 0
                    last_row_ind = 0
                    first_pay_date = dfSample2.iloc[i].date
                    if (i == 0):
                        last_row_ind = 0
                    elif (i == dfLengthCount):
                        last_row_ind = 1
                    elif (dfSample2.iloc[i].bankAcctID != dfSample2.iloc[i + 1].bankAcctID):
                        last_row_ind = 1

                # set values for final data set containing account ID, next pay date
                dfFinalSet1.loc[i, 'acct_id'] = dfSample2.iloc[i].bankAcctID
                dfFinalSet1.loc[i, 'cust_id'] = dfSample2.iloc[i].custID
                dfFinalSet1.loc[i, 'match_ind'] = dfSample2.iloc[i].match_ind
                dfFinalSet1.loc[i, 'fraudster'] = dfSample2.iloc[i].fraudster
                dfFinalSet1.loc[i, 'rightAcctFlag'] = dfSample2.iloc[i].rightAcctFlag
                dfFinalSet1.loc[i, 'cust_image_name'] = dfSample2.iloc[i].cust_image_name
                dfFinalSet1.loc[i, 'inc_no'] = days_increment
                dfFinalSet1.loc[i, 'pay_date'] = dfSample2.iloc[i].date
                dfFinalSet1.loc[i, 'weekday'] = dfSample2.iloc[i].date.weekday()
                dfFinalSet1.loc[i, 'transAmount'] = dfSample2.iloc[i].transAmount
                dfFinalSet1.loc[i, 'counter'] = last_row_ind
                if (inc_counter <= 0):
                    # dfFinalSet1.loc[i, 'next_pay_date'] = 'NA'
                    dfFinalSet1.loc[i, 'next_pay_flag'] = 'NA'
                else:
                    dfFinalSet1.loc[i, 'next_pay_date'] = dfSample2.iloc[i].date + days_increment
                    if (dfFinalSet1.iloc[i].next_pay_date.month == 4):
                        dfFinalSet1.loc[i, 'next_pay_date'] = dfFinalSet1.loc[i].next_pay_date + days_increment
                        if (first_pay_date.month == 4):
                            dfFinalSet1.loc[i, 'next_pay_date'] = first_pay_date + timedelta(days=days_in_month)
                            dfFinalSet1.loc[i, 'next_pay_flag'] = 'NA'
                        elif (first_pay_date.month == 3):
                            dfFinalSet1.loc[i, 'next_pay_date'] = dfSample2.iloc[i].date + timedelta(days=days_in_month)

                acct_id = dfSample2.iloc[i].bankAcctID

            dfSample5 = dfFinalSet1.loc[dfFinalSet1['acct_id'] == 207185]

            # The values in the List become the index of the new dataframe.
            # Setting these index as a column
            dfSample5['Count'] = dfSample5['inc_no'].index

            # Fetch the list of frequently repeated columns
            # list(dfSample5[dfSample5['inc_no'] == dfSample5.inc_no.max()]['Count'])
            # dfSample5['inc_no'] = dfSample5['inc_no'].timedelta.days.astype('int16') #.astype(int)
            # print(dfSample5.inc_no.max())
            # print(dfSample5)
            # exit()

            dfFinalSet1['next_pay_date'] = pd.to_datetime(dfFinalSet1['next_pay_date'], format="%Y-%m-%d %I:%M:%S")

            for i in range(0, len(dfFinalSet1)):
                if (dfFinalSet1.iloc[i].next_pay_date.weekday() == 5):
                    dfFinalSet1.loc[i, 'next_pay_date'] = dfFinalSet1.iloc[i].next_pay_date + timedelta(days=2)
                if (dfFinalSet1.iloc[i].next_pay_date.weekday() == 6):
                    dfFinalSet1.loc[i, 'next_pay_date'] = dfFinalSet1.iloc[i].next_pay_date + timedelta(days=1)

            dfFinalSet2 = dfFinalSet1.loc[dfFinalSet1['counter'] == 1]

            dfFinalSet3 = pd.DataFrame(columns=['cust_id', 'acct_id', 'next_pay_date', 'cust_image_name'])

            for i in range(0, len(dfFinalSet2) - 1):
                dfFinalSet3.loc[i, 'cust_id'] = dfFinalSet2.iloc[i].cust_id
                dfFinalSet3.loc[i, 'acct_id'] = dfFinalSet2.iloc[i].acct_id
                dfFinalSet3.loc[i, 'cust_image_name'] = dfFinalSet2.iloc[i].cust_image_name
                if (isNaN(dfFinalSet2.iloc[i].next_pay_date)):
                    dfFinalSet3.loc[i, 'next_pay_date'] = 'NA'
                else:
                    dfFinalSet3.loc[i, 'next_pay_date'] = dfFinalSet2.iloc[i].next_pay_date

            ########################################################################################################################
            ##MILESTONES 2 and 3 BLOCK - PREDICT FRAUDSTER AND RIGHT BANK ACCOUNT
            ########################################################################################################################

            # # import data from Live Customer list csv on to a dataframe
            dfLiveCustomers = pd.read_csv(localLiveCustomerListFileName,
                                          dtype={'custID': str, 'firstName': str, 'lastName': str})
            # # perform left outer join between Fraud Input and Live Customer dataframes
            dfResultLiveFraudCustomer = pd.merge(dfFinalSet3, dfLiveCustomers, left_on='cust_id', right_on='custID',
                                                 how='left')
            # print("Input list with next pay date joined to live customer list")
            # print(dfResultLiveFraudCustomer)

            # # import data from Fraud list csv on to a dataframe
            dfFraudList = pd.read_csv(localFraudListFileName, dtype={'firstName': str, 'lastName': str})
            #
            # # converting case of first and last names in both dataframes to Upper

            dfFraudList['firstName'] = dfFraudList['firstName'].str.upper()
            dfFraudList['lastName'] = dfFraudList['lastName'].str.upper()

            dfResultLiveFraudCustomer['firstName'] = dfResultLiveFraudCustomer['firstName'].str.upper()
            dfResultLiveFraudCustomer['lastName'] = dfResultLiveFraudCustomer['lastName'].str.upper()

            dfResultFraudCustomerList = pd.merge(dfResultLiveFraudCustomer,
                                                 dfFraudList[['firstName', 'lastName']],
                                                 on=['firstName', 'lastName'],
                                                 how='left',
                                                 indicator=True)

            dfResultFraudCustomerList.loc[dfResultFraudCustomerList['_merge'] == "left_only", 'fraudster'] = '0'
            dfResultFraudCustomerList.loc[dfResultFraudCustomerList['_merge'] == "both", 'fraudster'] = '1'
            # print(dfResultFraudCustomerList)

            # import data from Live Customer list csv on to a dataframe
            dfLiveCustomers = pd.read_csv(localLiveCustomerListFileName,
                                          dtype={'custID': str, 'firstName': str, 'lastName': str})
            # print(dfResultFraudCustomerList)

            dfResultMilestone2 = dfResultFraudCustomerList[
                ['cust_id', 'acct_id', 'fraudster', 'next_pay_date', 'cust_image_name']]
            # dfFinalResult = dfResultFraudCustomerList[['cust_id', 'fraudster']]

            dfResultLiveBankCustomer = pd.merge(dfResultMilestone2, dfLiveCustomers, left_on='cust_id',
                                                right_on='custID',
                                                how='left')

            # for col in dfResultLiveBankCustomer.columns:
            #    print(col)

            # import data from Fraud list csv on to a dataframe
            dfBankList = pd.read_csv(localLiveBankAccountListFileName,
                                     dtype={'bankAcctID': int, 'firstName': str, 'lastName': str})

            # converting case of first and last names in both dataframes to Upper

            dfBankList['firstName'] = dfBankList['firstName'].str.upper()
            dfBankList['lastName'] = dfBankList['lastName'].str.upper()

            dfResultLiveBankCustomer['firstName'] = dfResultLiveBankCustomer['firstName'].str.upper()
            dfResultLiveBankCustomer['lastName'] = dfResultLiveBankCustomer['lastName'].str.upper()

            # Join Live customer list to bank list on first name and last name
            dfResultLiveBankCustomerList = pd.merge(dfResultLiveBankCustomer,
                                                    dfBankList[['bankAcctID', 'firstName', 'lastName']],
                                                    on=['firstName', 'lastName'],
                                                    how='left',
                                                    indicator=True)
            # print("Input list after Fraud check tied to bank account list on first, last names")
            # print(dfResultLiveBankCustomerList)

            # print(dfResultLiveBankCustomerList.loc[dfResultLiveBankCustomerList['custID'] == 1599])

            # Join on Bank Account ID
            dfResultLiveBankCustomerOnBankAcct = pd.merge(dfResultLiveBankCustomer,
                                                          dfBankList[['bankAcctID', 'firstName', 'lastName']],
                                                          left_on='acct_id',
                                                          right_on='bankAcctID',
                                                          how='left',
                                                          indicator=True)
            # print(dfResultLiveBankCustomerOnBankAcct)

            # create an empty data frame to store final results
            dfResultMilestone3 = pd.DataFrame({'cust_id': pd.Series([], dtype='int'),
                                               'bankAcctID': pd.Series([], dtype='int'),
                                               'match_ind': pd.Series([], dtype='str'),
                                               'fraudster': pd.Series([], dtype='str'),
                                               'rightAcctFlag': pd.Series([], dtype='str'),
                                               'next_pay_date': pd.Series([], dtype='str'),
                                               'cust_image_name': pd.Series([], dtype='str')
                                               })

            # print("Input list after Fraud check tied to bank account list on first, last names - next joined on acct_id to get names out")
            # print(dfResultLiveBankCustomerOnBankAcct)

            for i in range(0, len(dfResultLiveBankCustomerOnBankAcct)):
                # compare first and last names on left side to right side to determine rightAcctFlag value
                if (dfResultLiveBankCustomerOnBankAcct.iloc[i].firstName_x == dfResultLiveBankCustomerOnBankAcct.iloc[
                    i].firstName_y
                        and dfResultLiveBankCustomerOnBankAcct.iloc[i].lastName_x ==
                        dfResultLiveBankCustomerOnBankAcct.iloc[i].lastName_y):
                    dfResultMilestone3.loc[i, 'cust_id'] = dfResultLiveBankCustomerOnBankAcct.iloc[i].custID
                    dfResultMilestone3.loc[i, 'bankAcctID'] = dfResultLiveBankCustomerOnBankAcct.iloc[i].acct_id
                    dfResultMilestone3.loc[i, 'cust_image_name'] = dfResultLiveBankCustomerOnBankAcct.iloc[
                        i].cust_image_name
                    dfResultMilestone3.loc[i, 'match_ind'] = ""
                    dfResultMilestone3.loc[i, 'fraudster'] = dfResultLiveBankCustomerOnBankAcct.iloc[i].fraudster
                    dfResultMilestone3.loc[i, 'next_pay_date'] = dfResultLiveBankCustomerOnBankAcct.iloc[
                        i].next_pay_date
                    dfResultMilestone3.loc[i, 'rightAcctFlag'] = "1"
                else:
                    dfResultMilestone3.loc[i, 'cust_id'] = dfResultLiveBankCustomerOnBankAcct.iloc[i].custID
                    dfResultMilestone3.loc[i, 'bankAcctID'] = dfResultLiveBankCustomerOnBankAcct.iloc[i].acct_id
                    dfResultMilestone3.loc[i, 'cust_image_name'] = dfResultLiveBankCustomerOnBankAcct.iloc[
                        i].cust_image_name
                    dfResultMilestone3.loc[i, 'match_ind'] = ""
                    dfResultMilestone3.loc[i, 'fraudster'] = dfResultLiveBankCustomerOnBankAcct.iloc[i].fraudster
                    dfResultMilestone3.loc[i, 'next_pay_date'] = dfResultLiveBankCustomerOnBankAcct.iloc[
                        i].next_pay_date
                    dfResultMilestone3.loc[i, 'rightAcctFlag'] = "0"

            dfResultMilestone3 = dfResultMilestone3.sort_values(by=['cust_id'])
            # print(dfResultMilestone3)
            # exit()

            ########################################################################################################################
            ##MILESTONES 1 - PREDICT FACE MATCH
            ########################################################################################################################

            key = "537f6c079b454e06b1fb4f8b4ae4bda8"
            endpoint = "https://m1faceapi.cognitiveservices.azure.com/"

            face_client = FaceClient(endpoint, CognitiveServicesCredentials(key))

            def get_face_id_from_image(image_name, folder_name):
                # change to working folder containing incoming images
                # print('Folder name {}-{}-{}'.format(folder_name,image_name,len(folder_name)))

                # if isNaN(folder_name):
                if len(folder_name) != 0:
                    # os.chdir(folder_name)
                    # IMAGES_FOLDER = os.path.join(os.path.dirname(os.path.realpath(__file__)))
                    IMAGES_FOLDER = folder_name
                    image_name = folder_name + '/' + image_name
                    # print('inside not null folder {} '.format(image_name))
                    test_image_array = glob.glob(image_name)
                    # test_image_array = glob.glob(image_name)
                else:
                    IMAGES_FOLDER = ''
                    # print('inside null folder {} '.format(image_name))
                    test_image_array = glob.glob(image_name)

                # Get test image
                # test_image_array = glob.glob(os.path.join(IMAGES_FOLDER, image_name))

                image = open(test_image_array[0], 'r+b')

                # Detect faces
                face_ids = []
                faces = face_client.face.detect_with_stream(image)
                for face in faces:
                    face_ids.append(face.face_id)

                image_face_ID = face_ids[0]
                # print('face id = {} '.format(str(image_face_ID)))
                return image_face_ID;

            path_to_zip_file = "/Users/tripasuri/work/zip"
            directory_to_extract_to = ""
            directory_source_to_extract_to = "data"
            localOutputFile = 'test.csv'

            # face_client = FaceClient(ENDPOINT, CognitiveServicesCredentials(KEY))
            face_client = FaceClient(endpoint, CognitiveServicesCredentials(key))

            # Set the FACE_ENDPOINT environment variable with the endpoint from your Face service in Azure.
            # This endpoint will be used in all examples in this quickstart.
            # image_file_path=dfFraudInput

            # go to directory_source_to_extract_to folder to fetch all images with matching cust ID
            # place all source image names and path in a dataframe
            dfSourceImages = pd.DataFrame(columns=['cust_id', 'image_name'])
            i = 0

            for file in glob.glob("data/*.jpg"):
                dfSourceImages.loc[i, 'cust_id'] = file[5:9]
                dfSourceImages.loc[i, 'image_name'] = file[5:19]
                i = i + 1

            # print(dfSourceImages)
            # join dfFraudInput with dfSourceImages to get matching source image files
            dfMatchingSourceImages = pd.merge(dfResultMilestone3,
                                              dfSourceImages[['cust_id', 'image_name']],
                                              on=['cust_id'],
                                              how='left',
                                              indicator=True)
            # print(dfMatchingSourceImages)

            # setting confidence as 0 for everyone
            dfMatchingSourceImages["confidence"] = 0
            # dfMatchingSourceImages["identical"] = 0

            # Image processing section

            # PUT THIS BLOCK BELOW IN A FUNCTION
            # Group image for testing against

            localtime = time.localtime()
            result = time.strftime("%I:%M:%S %p", localtime)
            # setting variables values of previous placeholders to 0
            j = 0
            prev_image_confidence = 0
            prev_cust_id = 0
            match_ind = 0
            prev_confidence = 0

            # print(dfMatchingSourceImages)
            # exit()

            # create an empty data frame to store final results
            dfFinalSet = pd.DataFrame(
                columns=['cust_id', 'acct_id', 'match_ind', 'next_pay_date', 'fraudster', 'rightAcctFlag'])
            lastRowInd = len(dfMatchingSourceImages) - 2
            # print(dfMatchingSourceImages)
            for i in range(0, len(dfMatchingSourceImages)):
                # print("Cust ID - {}, Fraudster - {}, Right Account flag - {}".format(dfMatchingSourceImages.loc[i].cust_id,dfMatchingSourceImages.loc[i].fraudster,dfMatchingSourceImages.loc[i].rightAcctFlag ))
                if (dfMatchingSourceImages.loc[i].fraudster == '0' and dfMatchingSourceImages.loc[
                    i].rightAcctFlag == '1'):
                    # check confidence of previous iteration image, if it is high skip the rest and set final match indicator to Yes
                    if (prev_image_confidence >= 0.5 and prev_cust_id == dfMatchingSourceImages.iloc[i].cust_id):
                        dfMatchingSourceImages.loc[i, 'confidence'] = verify_result_same.confidence
                        # if (i == lastRowInd):  # this is needed for the last cust ID in the list as it will be skipped
                        dfFinalSet.loc[j, 'cust_id'] = dfMatchingSourceImages.iloc[i - 1].cust_id
                        dfFinalSet.loc[j, 'acct_id'] = dfMatchingSourceImages.iloc[i - 1].bankAcctID
                        dfFinalSet.loc[j, 'match_ind'] = match_ind
                        dfFinalSet.loc[j, 'next_pay_date'] = dfMatchingSourceImages.iloc[i - 1].next_pay_date
                        dfFinalSet.loc[j, 'fraudster'] = dfMatchingSourceImages.iloc[i - 1].fraudster
                        dfFinalSet.loc[j, 'rightAcctFlag'] = dfMatchingSourceImages.iloc[i - 1].rightAcctFlag
                        #print("Final set 1 {} {} cust ID, match ind: {} {}".format(i, j, dfFinalSet.loc[j].cust_id,
                        #                                                           dfFinalSet.loc[j].match_ind))
                        j = j + 1
                        continue
                    # check confidence of previous iteration image, if it is low skip the rest and set final match indicator to No
                    # elif (prev_image_confidence < 0.5 and dfMatchingSourceImages.iloc[i + 1].cust_id != dfMatchingSourceImages.iloc[i].cust_id):
                    #    continue
                    # low confidence check was earlier 0.3 changed it 0.5 to save time
                    elif (prev_image_confidence < 0.5 and prev_cust_id == dfMatchingSourceImages.iloc[i].cust_id):
                        dfFinalSet.loc[j, 'cust_id'] = dfMatchingSourceImages.iloc[i - 1].cust_id
                        dfFinalSet.loc[j, 'acct_id'] = dfMatchingSourceImages.iloc[i - 1].bankAcctID
                        dfFinalSet.loc[j, 'match_ind'] = match_ind
                        dfFinalSet.loc[j, 'next_pay_date'] = dfMatchingSourceImages.iloc[i - 1].next_pay_date
                        dfFinalSet.loc[j, 'fraudster'] = dfMatchingSourceImages.iloc[i - 1].fraudster
                        dfFinalSet.loc[j, 'rightAcctFlag'] = dfMatchingSourceImages.iloc[i - 1].rightAcctFlag
                        #print("Final set 2 {} {} cust ID, match ind: {} {}".format(i, j, dfFinalSet.loc[j].cust_id,
                        #                                                           dfFinalSet.loc[j].match_ind))
                        j = j + 1
                        continue
                    elif (isNaN(dfMatchingSourceImages.iloc[i].image_name)):
                        dfFinalSet.loc[j, 'cust_id'] = dfMatchingSourceImages.iloc[i].cust_id
                        dfFinalSet.loc[j, 'acct_id'] = dfMatchingSourceImages.iloc[i].bankAcctID
                        dfFinalSet.loc[j, 'match_ind'] = 0
                        dfFinalSet.loc[j, 'next_pay_date'] = dfMatchingSourceImages.iloc[i].next_pay_date
                        dfFinalSet.loc[j, 'fraudster'] = dfMatchingSourceImages.iloc[i].fraudster
                        dfFinalSet.loc[j, 'rightAcctFlag'] = dfMatchingSourceImages.iloc[i].rightAcctFlag
                        #print("Final set 3 {} {} cust ID, match ind: {} {}".format(i, j, dfFinalSet.loc[j].cust_id,
                        #                                                           dfFinalSet.loc[j].match_ind))
                        j = j + 1
                        continue
                    # else:
                    #     if (i != 0):  # skip first iteration as there is nothing from previous iteration check
                    #         dfFinalSet.loc[j, 'cust_id'] = dfMatchingSourceImages.iloc[i - 1].cust_id
                    #         dfFinalSet.loc[j, 'match_ind'] = match_ind
                    #         print("Final set 4 {} {} cust ID, match ind: {} {}".format(i, j, dfFinalSet.loc[j].cust_id,dfFinalSet.loc[j].match_ind))
                    #         j = j + 1

                    # if (j >= 1):
                    #     print(
                    #         "Counter {} {} Cust ID {} Match ind {} {} ".format(j, i, dfMatchingSourceImages.iloc[i].cust_id,
                    #                                                            prev_confidence,
                    #                                                            dfMatchingSourceImages.iloc[i].image_name))

                    #print("Before calling API {}".format(dfMatchingSourceImages.iloc[i].cust_id))
                    # invoke face ID function call to fetch Face ID for input image from file
                    first_image_face_ID = get_face_id_from_image(
                        dfMatchingSourceImages.iloc[i].cust_image_name,
                        directory_to_extract_to)

                    # invoke face ID function call to fetch Face ID for source image from bank of images
                    second_image_face_ID = get_face_id_from_image(
                        dfMatchingSourceImages.iloc[i].image_name,
                        directory_source_to_extract_to)

                    # compare the two Face IDs using verify_face_to_face API call and save confidence value
                    verify_result_same = face_client.face.verify_face_to_face(first_image_face_ID, second_image_face_ID)
                    dfMatchingSourceImages.loc[i, 'confidence'] = verify_result_same.confidence

                    # save confidence and cust ID values for next iteration verification towards the top of this block in for loop
                    prev_image_confidence = verify_result_same.confidence
                    prev_cust_id = dfMatchingSourceImages.iloc[i].cust_id
                    prev_confidence = verify_result_same.confidence
                    # print(second_image_face_ID)
                    # set match indicator variable to be used for setting in final output dataframe
                    if (verify_result_same.confidence >= 0.5):
                        match_ind = 1
                    else:
                        match_ind = 0

                else:
                    if (prev_cust_id != dfMatchingSourceImages.iloc[i].cust_id):
                        print("in Non API section prev cust id - {}, cust_id - {}".format(prev_cust_id,
                                                                                          dfMatchingSourceImages.iloc[
                                                                                              i].cust_id))
                        dfFinalSet.loc[j, 'cust_id'] = dfMatchingSourceImages.iloc[i].cust_id
                        dfFinalSet.loc[j, 'acct_id'] = dfMatchingSourceImages.iloc[i].bankAcctID
                        dfFinalSet.loc[j, 'match_ind'] = 0
                        dfFinalSet.loc[j, 'next_pay_date'] = "NA"
                        dfFinalSet.loc[j, 'fraudster'] = dfMatchingSourceImages.iloc[i].fraudster
                        dfFinalSet.loc[j, 'rightAcctFlag'] = dfMatchingSourceImages.iloc[i].rightAcctFlag
                        prev_cust_id = dfMatchingSourceImages.iloc[i].cust_id
                        j = j + 1

            p_cust_id = 0
            dfFinalSetToCSV = pd.DataFrame(columns=['cust_id', 'next_pay_date'])

            # for i in range(0, len(dfMatchingSourceImages)):
            for i in range(0, len(dfFinalSet)):
                if (dfFinalSet.iloc[i].cust_id != p_cust_id):
                    dfFinalSetToCSV.loc[i, 'cust_id'] = dfFinalSet.iloc[i].cust_id
                    if (dfFinalSet.iloc[i].match_ind == 1):
                        dfFinalSetToCSV.loc[i, 'next_pay_date'] = dfFinalSet.iloc[i].next_pay_date
                    else:
                        dfFinalSetToCSV.loc[i, 'next_pay_date'] = "NA"
                    p_cust_id = dfFinalSet.iloc[i].cust_id

            # dfFinalSet3 = dfFinalSet2[['acct_id', 'next_pay_date']]
            dfFinalSetToCSV.to_csv(localFinalOutputFile, index=False, header=True)

            localtime_end = datetime.datetime.now()

            print(localtime_end)
            diff = localtime_end - localtime_begin
            iDiff = diff.total_seconds()
            #iDiff = iDiff / 1000000



        # return 'Time taken to execute in seconds is "{}". File name to download - {}'.format( iDiff,localFinalOutputFile)
        if (milestone_url > 5):
            #dfFinalSet.to_csv(localOutputFile, index=False, header=True)
            #return html.A('Download', id="download_link", href="/download_excel/")
            return html.Div([html.H6 ('Time taken in seconds {}'.format(iDiff)),
                             html.A('Download', id="download_link", href="/download_excel/")])
#            return html.A('Download', id="download_link", href="/download_excel/")
        else:
            html.Div('Link to download Excel will be displayed here', id='static_text'),

    # except (RuntimeError, TypeError, NameError):
    except TypeError as e:
        return 'The URL entered is invalid "{}"'.format(
            value

        )

if __name__ == '__main__':
    application.run(debug=True,port=8000)
