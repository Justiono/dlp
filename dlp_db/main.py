# developer: JP
# ENV VARS:
    # CV_PROJECT=
    # DB_COLLECTION1=
    # DB_COLLECTION2=
    # DLP_PROJECT=
    # DLP_TEMPLATE=

from flask import escape
import json, os, base64, requests
from datetime import datetime,date
import google.cloud.dlp
from google.cloud import firestore

project_id = os.environ.get('CV_PROJECT', 'CV_PROJECT environment variable is not set.') # "lt-aps-sbx"
topic_id = os.environ.get('CV_TOPIC', 'CV_TOPIC environment variable is not set.') # "add_new_patient"
dlp_project_id=os.environ.get('DLP_PROJECT', 'CV_PROJECT environment variable is not set.') # "lt-aps-sbx"
dlp_template_id=os.environ.get('DLP_TEMPLATE', 'CV_PROJECT environment variable is not set.') # "cv-dlp-poc"

def add_patient(patient_dict):
    db = firestore.Client()
    db_collection = os.environ.get('DB_COLLECTION1', 'Specified environment variable is not set.') #'Tok_Patients'
    #db_subcollection = os.environ.get('DB_SUBCOLLECTION', 'Specified environment variable is not set.') #'State_log'
    doc_id = patient_dict['patient']['guid']
    init_state_dict = {'state':'registered','update_time':str(datetime.now()),u'booster':'F',u'stateseq':'0'}
    patient_dict = {**patient_dict, **init_state_dict}
    #insert new patient
    db.collection(db_collection).document(doc_id).set(patient_dict)
    #add state_log subcollection
    #db.collection(db_collection).document(doc_id).collection(db_subcollection).document('0').set(init_state_dict)
    return json.dumps({ 'Result':'Success'})

def dlp_detokenize(patient_dict):
    #print(data_dict)
    enc_data = {
                "header":[
                    "regCode",
                    "dateOfBirth",
                    "healthCardNumber",
                    "emailAddress",
                    "phoneNumber",
                    "postalCode",
                    "streetNumber",
                    "unitNumber",
                    "streetName",
                    "postalCode2"
                ],
                "rows":[
                    [
                    patient_dict['patient']['regCode'],
                    patient_dict['patient']['dateOfBirth'],
                    patient_dict['patient']['patientData']['healthCardNumber'],
                    patient_dict['patient']['emailAddress'],
                    patient_dict['patient']['phoneNumber'],
                    patient_dict['patient']['postalCode'],
                    patient_dict['patient']['patientAddressData']['streetNumber'],
                    patient_dict['patient']['patientAddressData']['unitNumber'],
                    patient_dict['patient']['patientAddressData']['streetName'],
                    patient_dict['patient']['patientAddressData']['postalCode']
                    ]
                ]
            }

    headers = [{"name": val} for val in enc_data["header"]]
    rows = []
    for row in enc_data["rows"]:
        rows.append( {"values": [{"string_value": cell_val} for cell_val in row]} )
    table = {}
    table["headers"] = headers
    table["rows"] = rows
    item = {"table": table}
    dlp = google.cloud.dlp.DlpServiceClient()
    parent = dlp.project_path(project_id)
    deidentify_template=f"projects/{dlp_project_id}/deidentifyTemplates/{dlp_template_id}"
    response = dlp.reidentify_content(parent, reidentify_template_name=deidentify_template,item=item)
    detok_patient = response.item.table.rows[0].values
    #print(response.item)
    patient_dict['patient']['regCode'] = str(detok_patient[0].string_value)
    patient_dict['patient']['dateOfBirth'] = str(detok_patient[1].string_value)
    patient_dict['patient']['patientData']['healthCardNumber'] = str(detok_patient[2].string_value)
    patient_dict['patient']['emailAddress'] = str(detok_patient[3].string_value)
    patient_dict['patient']['phoneNumber'] = str(detok_patient[4].string_value)
    patient_dict['patient']['postalCode'] = str(detok_patient[5].string_value)
    patient_dict['patient']['patientAddressData']['streetNumber'] = str(detok_patient[6].string_value)
    patient_dict['patient']['patientAddressData']['unitNumber'] = str(detok_patient[7].string_value)
    patient_dict['patient']['patientAddressData']['streetName'] = str(detok_patient[8].string_value)
    patient_dict['patient']['patientAddressData']['postalCode'] = str(detok_patient[9].string_value)
    return patient_dict

def add_detok_patient(patient_dict):
    db = firestore.Client()
    db_collection = os.environ.get('DB_COLLECTION2', 'Specified environment variable is not set.') #'Detok_Patients'
    doc_id = patient_dict['patient']['guid']
    #insert new patient
    db.collection(db_collection).document(doc_id).set(patient_dict)
    return json.dumps({ 'Result':'Success'})

def http_add_patient(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    """
    request_json = request.get_json(silent=True)
    #request_args = request.args
    
    data = json.loads(base64.b64decode(request_json['message']['data']).decode('utf-8')) # dict type
    data['header'] = {}
    data['header']['messageId'] = request_json['message']['messageId']
    data['header']['publishTime'] = request_json['message']['publishTime']

    Result = add_patient(data)
    unprotected_data = dlp_detokenize(data)
    Result = add_detok_patient(unprotected_data)
    return Result

#requirements.txt
#google-cloud-firestore>=2.0.0
#firebase-admin>=3.2.1
#google-cloud-dlp==1.0.0


#set GOOGLE_APPLICATION_CREDENTIALS=C:\python_codes\lt-aps-sbx-6a1381ce2963.json
#= 'C:\python_codes\access\lt-aps-sbx-dlp-6093342f78be.json'
