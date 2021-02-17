# developer: JP
# ENV VARS:
    # CV_PROJECT=
    # DLP_PROJECT=
    # DLP_TEMPLATE=
    # PROTEGRITY_ENDPOINT=
    # DEST_PUBSUB_PROJECT=
    # DEST_PUBSUB_TOPIC=

import json, os, base64, requests
from datetime import datetime,date
import google.cloud.dlp
from google.cloud import pubsub_v1

project_id = os.environ.get('CV_PROJECT', 'CV_PROJECT environment variable is not set.') # "lt-aps-sbx"
topic_id = os.environ.get('CV_TOPIC', 'CV_TOPIC environment variable is not set.') # "add_new_patient"
dlp_project_id = os.environ.get('DLP_PROJECT', 'CV_PROJECT environment variable is not set.') # "lt-aps-sbx"
dlp_template_id = os.environ.get('DLP_TEMPLATE', 'CV_PROJECT environment variable is not set.') # "cv-dlp-poc"
protegrity_endpoint = os.environ.get('PROTEGRITY_ENDPOINT', 'PROTEGRITY_ENDPOINT environment variable is not set.') # https://10.194.140.100:443/data-management/protect
dest_pubsub_project = os.environ.get('DEST_PUBSUB_PROJECT', 'DEST_PUBSUB_PROJECT environment variable is not set.')
dest_pubsub_topic = os.environ.get('DEST_PUBSUB_TOPIC', 'DEST_PUBSUB_TOPIC environment variable is not set.')

def dlp_detokenize(patient_dict):
    #print(data_dict)
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

def protegrity_tokenize(data_dict):
    #url = 'https://10.194.140.100:443/data-management/protect'
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}

    def do_tokenize(data_str):
        #data = json.dumps(data_str)
        response = requests.post(protegrity_endpoint, data=data_str,verify=False, headers=headers)
        if response:
            #print('Request is successful.\n\n')
            return json.loads(response.text)
        else:
            print('Request returned an error.')
            return { 'error_code':response.status_code }

    #One pass to Protegrity
    data = '[{\"name\":\"' + data_dict['patient']['regCode'] + '\",\"addressline1\":\"' + data_dict['patient']['dateOfBirth'] + '\",\"creditinformation\":\"' + data_dict['patient']['patientData']['healthCardNumber'] + '\",\"postalcode\":\"' + data_dict['patient']['emailAddress'] + '\"},{\"name\":\"' + data_dict['patient']['phoneNumber'] + '\",\"addressline1\":\"' + data_dict['patient']['postalCode'] + '\",\"creditinformation\":\"' + data_dict['patient']['patientAddressData']['streetNumber'] + '\",\"postalcode\":\"' + data_dict['patient']['patientAddressData']['unitNumber'] + '\"},{\"name\":\"' + data_dict['patient']['patientAddressData']['streetName'] + '\",\"addressline1\":\"' + data_dict['patient']['patientAddressData']['postalCode'] + '\"}]'
    protected_data = do_tokenize(data)
    if protected_data:
        data_dict['patient']['regCode'] = protected_data[0]['name']
        data_dict['patient']['dateOfBirth'] = protected_data[0]['addressline1']
        data_dict['patient']['patientData']['healthCardNumber'] = protected_data[0]['creditinformation']
        data_dict['patient']['emailAddress'] = protected_data[0]['postalcode']
        data_dict['patient']['phoneNumber'] = protected_data[1]['name']
        data_dict['patient']['postalCode'] = protected_data[1]['addressline1']
        data_dict['patient']['patientAddressData']['streetNumber'] = protected_data[1]['creditinformation']
        data_dict['patient']['patientAddressData']['unitNumber'] = protected_data[1]['postalcode']
        data_dict['patient']['patientAddressData']['streetName'] = protected_data[2]['name']
        data_dict['patient']['patientAddressData']['postalCode'] = protected_data[2]['addressline1']
    else:
        print('Tokenization returned an error.')
        return { 'error_code:900' }
    return data_dict

#requirements => google-cloud-pubsub
def publish_patient(patient_str):
    """Publish patient to the Pub/Sub Topic."""
    
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(dest_pubsub_project, dest_pubsub_topic)

    data = json.dumps(patient_str).encode("utf-8")

    try:
        future = publisher.publish(topic_path, data)
        msg_id = future.result()
        print('Message is published with message id:{}'.format(msg_id))
    except Exception as e:
        print('Error. Message id:{}, Error message:{}'.format(msg_id, e))
        return json.dumps({ "Result":500, "errmsg":e })
    return json.dumps({ "Result":"OK" })

def http_detok_patient(request):
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
    
    data_dict = json.loads(base64.b64decode(request_json['message']['data']).decode('utf-8'))
    data_dict['header'] = {}
    data_dict['header']['messageId'] = request_json['message']['messageId']
    data_dict['header']['publishTime'] = request_json['message']['publishTime']
    #print(data_dict)

    patient_dict = dlp_detokenize(data_dict)
    #print(patient_dict)

    #retokenize with Protegrity
    prt_patient_dict = protegrity_tokenize(patient_dict)  # return a dict
    prt_patient_dict = patient_dict
    #Publish to Pub/Sub Topic
    try:
        publish_patient(prt_patient_dict)
    except Exception as e:
        print('Error.')
        return json.dumps({ "Result":500, "errmsg":e })
    return {'Result':'ok'}

#requirements.txt
#google-cloud-dlp==1.0.0
#google-cloud-pubsub
#requests

#set GOOGLE_APPLICATION_CREDENTIALS=C:\python_codes\lt-aps-sbx-6a1381ce2963.json
#= 'C:\python_codes\access\lt-aps-sbx-dlp-6093342f78be.json'
